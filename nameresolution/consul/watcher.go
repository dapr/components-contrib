package consul

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	consul "github.com/hashicorp/consul/api"
)

const (
	// retryInterval is the base retry value.
	retryInterval = 5 * time.Second

	// maximum back off time, this is to prevent exponential runaway.
	maxBackoffTime = 180 * time.Second
)

type watchPlan struct {
	expired      bool
	lastParamVal blockingParamVal
	lastResult   []*consul.ServiceEntry
	service      string
	options      *consul.QueryOptions
}

type blockingParamVal interface {
	equal(other blockingParamVal) bool
	next(previous blockingParamVal) blockingParamVal
}

type waitIndexVal uint64

// Equal implements BlockingParamVal.
func (idx waitIndexVal) equal(other blockingParamVal) bool {
	if otherIdx, ok := other.(waitIndexVal); ok {
		return idx == otherIdx
	}

	return false
}

// Next implements BlockingParamVal.
func (idx waitIndexVal) next(previous blockingParamVal) blockingParamVal {
	if previous == nil {
		return idx
	}
	prevIdx, ok := previous.(waitIndexVal)
	if ok && prevIdx == idx {
		// This value is the same as the previous index, reset
		return waitIndexVal(0)
	}

	return idx
}

func (r *resolver) watch(p *watchPlan) (blockingParamVal, []*consul.ServiceEntry, bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p.options = p.options.WithContext(ctx)

	if p.lastParamVal != nil {
		p.options.WaitIndex = uint64(p.lastParamVal.(waitIndexVal))
	}

	defer cancel()
	nodes, meta, err := r.client.Health().Service(p.service, "", true, p.options)

	// If error try again without blocking (for stale agent cache)
	if err != nil && p.options.WaitIndex != uint64(0) {
		p.options.WaitIndex = 0
		nodes, meta, err = r.client.Health().Service(p.service, "", true, p.options)
	}

	if err != nil {
		if p.options.WaitIndex == uint64(0) && !p.expired {
			p.lastResult = nil
			p.expired = true
			r.registry.expire(p.service)
		}

		return nil, nil, false, err
	} else if meta.CacheHit && meta.CacheAge > 0 {
		err = fmt.Errorf("agent cache is stale (age %s)", meta.CacheAge.String())
		p.expired = false

		return nil, nodes, true, err
	}

	p.expired = false

	return waitIndexVal(meta.LastIndex), nodes, false, err
}

func (r *resolver) runWatchPlan(p *watchPlan) {
	defer func() {
		recover()
		r.registry.remove(p.service)
	}()

	// add to registry as now begun watching
	r.registry.addOrUpdate(p.service, nil)
	failures := 0

	for {
		// invoke blocking call
		blockParam, result, stale, err := r.watch(p)
		// handle an error in the watch function
		if err != nil {
			// perform an exponential backoff
			failures++

			// always 0 on err so query is forced to return and set p.healthy!
			p.lastParamVal = waitIndexVal(0)

			retry := retryInterval * time.Duration(failures*failures)
			if retry > maxBackoffTime {
				retry = maxBackoffTime
			}

			r.logger.Errorf("consul service-watcher:%s error: %v, retry in %v", p.service, err, retry)

			if stale {
				r.logger.Debugf("updating registry for service:%s using stale cache", p.service)
				r.registry.addOrUpdate(p.service, result)
			}

			time.Sleep(retry)

			continue
		}

		// clear the failures
		failures = 0

		// if the index is unchanged do nothing
		if p.lastParamVal != nil && p.lastParamVal.equal(blockParam) {
			continue
		}

		// update the index, look for change
		oldParamVal := p.lastParamVal
		p.lastParamVal = blockParam.next(oldParamVal)
		if oldParamVal != nil && reflect.DeepEqual(p.lastResult, result) {
			continue
		}

		// handle the updated result
		p.lastResult = result
		r.logger.Debugf(
			"updating registry for service:%s last-index:%s",
			p.service,
			strconv.FormatUint(uint64(p.lastParamVal.(waitIndexVal)), 10))
		r.registry.addOrUpdate(p.service, result)
	}
}

func (r *resolver) watchService(service string) {
	options := *r.config.QueryOptions
	plan := &watchPlan{
		service: service,
		options: &options,
	}

	go r.runWatchPlan(plan)
}
