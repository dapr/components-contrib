package health

import "context"

type Pinger interface {
	Ping(ctx context.Context) error
}
