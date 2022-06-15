package health

type Pinger interface {
	Ping() error
}
