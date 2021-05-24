package nsq

import (
	gnsq "github.com/nsqio/go-nsq"
)

type metadata struct {
	lookupdAddrs []string
	addrs        []string
	config       *gnsq.Config
}

type subscriber struct {
	topic string

	c *gnsq.Consumer

	// handler so we can resubcribe
	h gnsq.HandlerFunc
	// concurrency
	n int
}
