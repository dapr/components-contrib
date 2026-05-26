// Test-only JWT-validating RESP proxy used by the state/redis useOIDC
// certification test. Listens on $LISTEN_ADDR (RESP), expects the first
// command from the client to be AUTH (or HELLO with inline AUTH), validates
// the JWT against $JWKS_URL, rewrites the AUTH password to the configured
// $BACKEND_PASSWORD, and then transparently bridges bytes to $VALKEY_ADDR.
//
// Stands in for JPMC's auth-proxy in front of Valkey. By having Valkey handle
// the AUTH (with the rewritten password) the proxy doesn't need to synthesise
// responses, so pipelined commands stay in sync.
package main

import (
	"bufio"
	"bytes"
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
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// proxyState holds the JWKS keyfunc once it's been fetched. Until then,
// AUTH requests are rejected with a NOTREADY error rather than blocking,
// so the container can be marked "running" by docker compose immediately.
type proxyState struct {
	mu sync.RWMutex
	kf keyfunc.Keyfunc
}

func (s *proxyState) get() keyfunc.Keyfunc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.kf
}

func (s *proxyState) set(kf keyfunc.Keyfunc) {
	s.mu.Lock()
	s.kf = kf
	s.mu.Unlock()
}

func main() {
	listenAddr := mustenv("LISTEN_ADDR")
	valkeyAddr := mustenv("VALKEY_ADDR")
	jwksURL := mustenv("JWKS_URL")
	backendPass := mustenv("BACKEND_PASSWORD")
	expectedAud := os.Getenv("EXPECTED_AUDIENCE")

	state := &proxyState{}
	go initJWKS(state, jwksURL)

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
		go handle(conn, valkeyAddr, backendPass, state, expectedAud)
	}
}

func initJWKS(state *proxyState, jwksURL string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for {
		if _, err := http.Get(jwksURL); err == nil {
			kf, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
			if err == nil {
				state.set(kf)
				log.Printf("JWKS keyfunc ready (%s)", jwksURL)
				return
			}
			log.Printf("keyfunc init failed (will retry): %v", err)
		}
		if ctx.Err() != nil {
			log.Printf("gave up waiting for JWKS at %s", jwksURL)
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// handle services a single client connection. It expects the first RESP
// command to be either AUTH (legacy) or HELLO with inline AUTH (go-redis
// v9 default). After validating the JWT-as-password, it rewrites the AUTH
// password to the configured backend password and forwards the full
// command to Valkey. Subsequent traffic is bridged byte-for-byte so that
// pipelined commands and their responses remain in lock-step.
func handle(client net.Conn, valkeyAddr, backendPass string, state *proxyState, expectedAud string) {
	defer client.Close()

	br := bufio.NewReader(client)
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	cmd, err := readRESPArray(br)
	_ = client.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("read failed: %v", err)
		return
	}
	if len(cmd) == 0 {
		_ = writeError(client, "ERR empty command")
		return
	}

	head := strings.ToUpper(cmd[0])
	var rewritten []string

	switch head {
	case "AUTH":
		// AUTH password  OR  AUTH username password
		if len(cmd) < 2 {
			_ = writeError(client, "ERR wrong number of arguments for 'AUTH'")
			return
		}
		password := cmd[len(cmd)-1]
		if err := validateJWT(password, state, expectedAud); err != nil {
			_ = writeError(client, err.Error())
			return
		}
		log.Print("authn (AUTH): token valid")
		// Replace just the password component; preserve any username arg.
		rewritten = append([]string(nil), cmd...)
		rewritten[len(rewritten)-1] = backendPass

	case "HELLO":
		// HELLO [protover [AUTH user pass] [SETNAME name]]
		authIdx := -1
		for i := 1; i < len(cmd); i++ {
			if strings.EqualFold(cmd[i], "AUTH") {
				authIdx = i
				break
			}
		}
		if authIdx < 0 || authIdx+2 >= len(cmd) {
			// HELLO without inline AUTH — tell client we don't support
			// it so go-redis falls back to plain AUTH on the same conn.
			_ = writeError(client, "ERR HELLO without inline AUTH is not supported by this proxy")
			// Drop the connection; go-redis will reconnect and send AUTH directly.
			return
		}
		password := cmd[authIdx+2]
		if err := validateJWT(password, state, expectedAud); err != nil {
			_ = writeError(client, err.Error())
			return
		}
		log.Print("authn (HELLO): token valid")
		rewritten = append([]string(nil), cmd...)
		rewritten[authIdx+2] = backendPass

	default:
		_ = writeError(client, "NOAUTH Authentication required.")
		return
	}

	backend, err := net.Dial("tcp", valkeyAddr)
	if err != nil {
		log.Printf("backend dial failed: %v", err)
		return
	}
	defer backend.Close()

	if _, err := backend.Write(encodeRESPArray(rewritten)); err != nil {
		log.Printf("backend write (rewritten cmd) failed: %v", err)
		return
	}

	// Drain anything bufio buffered alongside the first command (pipelined
	// commands sent in the same TCP segment).
	if n := br.Buffered(); n > 0 {
		peek, _ := br.Peek(n)
		if _, err := backend.Write(peek); err != nil {
			log.Printf("backend write (drain) failed: %v", err)
			return
		}
		_, _ = br.Discard(n)
	}

	// Transparent bidirectional bridge. Valkey produces the response to
	// AUTH/HELLO (with the rewritten password) and to every subsequent
	// command, so the pipeline stays in lock-step.
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

func validateJWT(raw string, state *proxyState, expectedAud string) error {
	kf := state.get()
	if kf == nil {
		return errors.New("NOTREADY proxy is still initialising JWKS")
	}
	opts := []jwt.ParserOption{jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"})}
	if expectedAud != "" {
		opts = append(opts, jwt.WithAudience(expectedAud))
	}
	tok, err := jwt.Parse(raw, kf.Keyfunc, opts...)
	if err != nil {
		return fmt.Errorf("WRONGPASS invalid token: %w", err)
	}
	if !tok.Valid {
		return errors.New("WRONGPASS token not valid")
	}
	return nil
}

// readRESPArray parses a single RESP array command (e.g. AUTH <pass>).
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

// encodeRESPArray serialises a string slice as a RESP array of bulk strings.
func encodeRESPArray(args []string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(a), a)
	}
	return buf.Bytes()
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
