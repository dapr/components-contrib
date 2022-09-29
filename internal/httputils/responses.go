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
	RespondWithErrorAndMessage(w, statusCode, statusText)
}

// RespondWithErrorAndMessage responds to a http.ResponseWriter with an error status code.
// The message is included in the body as response.
// This method should be invoked before calling w.WriteHeader, and callers should abort the request after calling this method.
func RespondWithErrorAndMessage(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	w.Write([]byte(message))
}

// RespondWithRedirect responds to a http.ResponseWriter with a redirect.
// This method should be invoked before calling w.WriteHeader, and callers should abort the request after calling this method.
func RespondWithRedirect(w http.ResponseWriter, statusCode int, location string) {
	w.Header().Set("location", location)
	w.WriteHeader(statusCode)
}
