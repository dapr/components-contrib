package httputils

import (
	"net/http"
)

// RespondWithError responds to a http.ResponseWriter with an error status code.
// The text corresponding to the status code is sent as body of the response.
// This method should be invoked before calling w.WriteHeader, and callers should abort the request after calling this method.
func RespondWithError(w http.ResponseWriter, statusCode int) {
	statusText := http.StatusText(statusCode)
	if statusText == "" {
		statusCode = http.StatusInternalServerError
		statusText = http.StatusText(statusCode)
	}
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	w.Write([]byte(statusText))
}
