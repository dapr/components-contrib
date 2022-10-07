package httputils

import "net/http"

// RequestURI returns the path and query string (if present) from the request
// For example: `/foo` or `/foo?hello=world`
func RequestURI(r *http.Request) string {
	u := r.URL.Path
	if len(u) == 0 || u[0] != '/' {
		u = "/" + u
	}
	if len(r.URL.RawQuery) > 0 {
		u += "?" + r.URL.RawQuery
	}
	return u
}
