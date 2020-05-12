package custom

import (
	"google.golang.org/grpc"
)

// Custom registers a custom component.
type Custom interface {
	Init(metadata Metadata) error
	RegisterServer(s *grpc.Server) error
}
