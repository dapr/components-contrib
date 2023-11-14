package httputils

import (
	"net/http"
	"net/url"
	"testing"
)

func TestRequestURI(t *testing.T) {
	tests := []struct {
		name        string
		url         *url.URL
		expectedURI string
	}{
		{
			name: "coerces empty path to slash",
			url: &url.URL{
				Scheme: "http",
				Host:   "example.com",
				Path:   "",
			},
			expectedURI: "/",
		},
		{
			name: "encodes space",
			url: &url.URL{
				Scheme: "http",
				Host:   "example.com",
				Path:   "/a b",
			},
			expectedURI: "/a%20b",
		},
		{
			name: "encodes query",
			url: &url.URL{
				Scheme:   "http",
				Host:     "example.com",
				Path:     "/a b",
				RawQuery: "q=go+language",
			},
			expectedURI: "/a%20b?q=go+language",
		},
		{
			name: "double slash path",
			url: &url.URL{
				Scheme: "http",
				Host:   "example.com",
				Path:   "//foo",
			},
			expectedURI: "//foo",
		},
		{
			name: "empty query",
			url: &url.URL{
				Scheme:     "http",
				Host:       "example.com",
				Path:       "/foo",
				ForceQuery: true,
			},
			expectedURI: "/foo?",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{URL: tc.url}
			if want, have := tc.expectedURI, RequestURI(r); want != have {
				t.Errorf("unexpected uri, want: %s, have: %s", want, have)
			}
		})
	}
}

func Test_SetRequestURI(t *testing.T) {
	tests := []struct {
		name        string
		expectedURI string
	}{
		{
			name:        "coerces empty path to slash",
			expectedURI: "/",
		},
		{
			name:        "encodes space",
			expectedURI: "/a%20b",
		},
		{
			name:        "encodes query",
			expectedURI: "/a%20b?q=go+language",
		},
		{
			name:        "double slash path",
			expectedURI: "//foo",
		},
		{
			name:        "empty query",
			expectedURI: "/foo?",
		},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{URL: &url.URL{}}
			if err := SetRequestURI(r, tc.expectedURI); err != nil {
				t.Error(err)
			}
			if want, have := tc.expectedURI, r.URL.RequestURI(); want != have {
				t.Errorf("unexpected uri, want: %s, have: %s", want, have)
			}
		})
	}
}
