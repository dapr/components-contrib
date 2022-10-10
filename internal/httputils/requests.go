package httputils

import (
	"net/http"
	"net/url"
)

// RequestURI returns the path and query string (if present) from the request
// For example: `/foo` or `/foo?hello=world`
func RequestURI(r *http.Request) string {
	u := r.URL
	result := u.EscapedPath()
	if result == "" {
		result = "/"
	}
	if u.ForceQuery || u.RawQuery != "" {
		result += "?" + u.RawQuery
	}
	return result
}

// SetRequestURI replaces the path and query string (if present) from the
// request with the input. For example: `/foo` or `/foo?hello=world`
func SetRequestURI(r *http.Request, uri string) error {
	if u, err := url.ParseRequestURI(uri); err != nil {
		return err
	} else { // copy the URI without overwriting the host, etc.
		r.URL.RawPath = u.RawPath
		r.URL.Path = u.Path
		r.URL.ForceQuery = u.ForceQuery
		r.URL.RawQuery = u.RawQuery
	}
	return nil
}
