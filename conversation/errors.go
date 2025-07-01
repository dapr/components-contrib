package conversation

import "errors"

var (
	ErrToolCallStreamingNotSupported = errors.New("tool call streaming is not supported by this model or provider")
	ErrStreamingNotSupported         = errors.New("streaming is not supported by this model or provider")
)
