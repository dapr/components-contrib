// Test-only JWT-validating RESP proxy used by the state/redis useOIDC
// certification test. Listens on $LISTEN_ADDR (RESP), expects the first
// command from the client to be AUTH with a JWT as the password, validates
// the JWT against $JWKS_URL, and on success transparently forwards bytes
// to $VALKEY_ADDR.
//
// This stands in for JPMC's auth-proxy in front of Valkey. The
// implementation deliberately does the minimum needed to exercise the
// useOIDC flow end-to-end: no command parsing beyond AUTH, no per-command
// re-validation, no connection pooling on the backend side.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

func main() {
	listenAddr := mustenv("LISTEN_ADDR")
	valkeyAddr := mustenv("VALKEY_ADDR")
	jwksURL := mustenv("JWKS_URL")
	expectedAud := os.Getenv("EXPECTED_AUDIENCE")

	// Wait for the JWKS endpoint to come up; the IDP usually takes longer
	// than the proxy to be ready.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for {
		if _, err := http.Get(jwksURL); err == nil {
			break
		}
		if ctx.Err() != nil {
			log.Fatalf("JWKS endpoint never became reachable: %s", jwksURL)
		}
		time.Sleep(2 * time.Second)
	}

	kf, err := keyfunc.NewDefaultCtx(context.Background(), []string{jwksURL})
	if err != nil {
		log.Fatalf("could not initialize JWKS keyfunc: %v", err)
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	log.Printf("jwt-resp-proxy listening on %s; valkey=%s; jwks=%s", listenAddr, valkeyAddr, jwksURL)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handle(conn, valkeyAddr, kf, expectedAud)
	}
}

func handle(client net.Conn, valkeyAddr string, kf keyfunc.Keyfunc, expectedAud string) {
	defer client.Close()
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))

	br := bufio.NewReader(client)
	cmd, err := readRESPArray(br)
	if err != nil {
		log.Printf("read failed: %v", err)
		return
	}
	_ = client.SetReadDeadline(time.Time{})

	if len(cmd) < 2 || !strings.EqualFold(cmd[0], "AUTH") {
		_ = writeError(client, "NOAUTH Authentication required.")
		return
	}
	// Accept both AUTH <pass> and AUTH <user> <pass> forms.
	password := cmd[len(cmd)-1]

	if err := validate(password, kf, expectedAud); err != nil {
		log.Printf("authn rejected: %v", err)
		_ = writeError(client, "WRONGPASS invalid token: "+err.Error())
		return
	}
	log.Print("authn: token valid")

	if _, err := io.WriteString(client, "+OK\r\n"); err != nil {
		return
	}

	backend, err := net.Dial("tcp", valkeyAddr)
	if err != nil {
		log.Printf("backend dial failed: %v", err)
		return
	}
	defer backend.Close()

	// If anything was buffered in the bufio.Reader after the AUTH command
	// (e.g. a pipelined SELECT/INFO), flush it to the backend before
	// starting the bidirectional copy.
	if n := br.Buffered(); n > 0 {
		buf, _ := br.Peek(n)
		if _, err := backend.Write(buf); err != nil {
			return
		}
		_, _ = br.Discard(n)
	}

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(backend, br)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, backend)
		done <- struct{}{}
	}()
	<-done
}

func validate(raw string, kf keyfunc.Keyfunc, expectedAud string) error {
	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"})}
	if expectedAud != "" {
		opts = append(opts, jwt.WithAudience(expectedAud))
	}
	tok, err := jwt.Parse(raw, kf.Keyfunc, opts...)
	if err != nil {
		return err
	}
	if !tok.Valid {
		return errors.New("token not valid")
	}
	return nil
}

// readRESPArray parses a single RESP array command (e.g. AUTH password).
func readRESPArray(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("bad array length: %w", err)
	}
	args := make([]string, n)
	for i := 0; i < n; i++ {
		hdr, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, fmt.Errorf("expected bulk string, got %q", hdr)
		}
		ln, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, fmt.Errorf("bad bulk length: %w", err)
		}
		buf := make([]byte, ln+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:ln])
	}
	return args, nil
}

func writeError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", msg)
	return err
}

func mustenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env: %s", k)
	}
	return v
}
