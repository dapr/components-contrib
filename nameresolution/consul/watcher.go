package consul

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	expired               bool
	lastParamVal          blockingParamVal
	lastResult            map[serviceIdentifier]bool
	options               *consul.QueryOptions
	configuredQueryFilter string
	failures              int
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

type serviceIdentifier struct {
	serviceName string
	serviceID   string
	node        string
}

func getServiceIdentifier(c *consul.HealthCheck) serviceIdentifier {
	return serviceIdentifier{
		serviceID:   c.ServiceID,
		serviceName: c.ServiceName,
		node:        c.Node}
}

func getHealthByService(checks consul.HealthChecks) map[serviceIdentifier]bool {
	healthByService := make(map[serviceIdentifier]bool)
	for _, check := range checks {
		id := getServiceIdentifier(check)

		if state, ok := healthByService[id]; !ok {
			// Init to healthy
			healthByService[id] = true
		} else if !state {
			continue
		}

		if check.Status != consul.HealthPassing {
			healthByService[id] = false
		}
	}

	return healthByService
}

func (p *watchPlan) getChangedServices(newResult map[serviceIdentifier]bool) map[string]struct{} {
	changedServices := make(map[string]struct{})

	// get changed services
	for newKey, newValue := range newResult {
		if oldValue, ok := p.lastResult[newKey]; ok && newValue == oldValue {
			continue
		}

		changedServices[newKey.serviceName] = struct{}{}
	}

	for oldKey := range p.lastResult {
		if _, ok := newResult[oldKey]; !ok {
			changedServices[oldKey.serviceName] = struct{}{}
		}
	}

	return changedServices
}

func (p *watchPlan) buildServiceNameFilter(services []string) {
	var nameFilters = make([]string, len(services))

	for i, v := range services {
		nameFilters[i] = `ServiceName=="`+v+`"`
	}

	filter := "(" + strings.Join(nameFilters, " or ") + ")"

	if len(p.configuredQueryFilter) < 1 {
		p.options.Filter = filter
	} else {
		p.options.Filter = fmt.Sprintf("%s and %s", p.configuredQueryFilter, filter)
	}
}

func (r *resolver) watch(p *watchPlan, services []string, ctx context.Context) (blockingParamVal, consul.HealthChecks, error, bool) {
	p.options = p.options.WithContext(ctx)

	if p.lastParamVal != nil {
		p.options.WaitIndex = uint64(p.lastParamVal.(waitIndexVal))
	}

	p.buildServiceNameFilter(services)

	checks, meta, err := r.client.Health().State(consul.HealthAny, p.options)

	if err != nil {
		// if it failed during long poll try again with no wait
		if p.options.WaitIndex != uint64(0) {
			p.options.WaitIndex = 0
			checks, meta, err = r.client.Health().State(consul.HealthAny, p.options)
		}

		if err != nil {
			// if the context was canceled
			if errors.Is(err, context.Canceled) {
				return nil, nil, nil, true
			}

			// if it failed with no wait and plan is not expired
			if p.options.WaitIndex == uint64(0) && !p.expired {
				p.lastResult = nil
				p.expired = true
				r.registry.expireAll()
			}

			return nil, nil, err, false
		}
	}

	p.expired = false

	return waitIndexVal(meta.LastIndex), checks, err, false
}

func (r *resolver) runWatchPlan(p *watchPlan, services []string, ctx context.Context, watchTask chan bool) {
	defer func() {
		recover()
		// complete watch task
		watchTask <- true
	}()

	// invoke blocking call
	blockParam, result, err, canceled := r.watch(p, services, ctx)

	// if the ctx was canceled then do nothing
	if canceled {
		return
	}

	// handle an error in the watch function
	if err != nil {
		// always 0 on err so query is forced to return
		p.lastParamVal = waitIndexVal(0)

		// perform an exponential backoff
		p.failures++

		retry := retryInterval * time.Duration(p.failures*p.failures)
		if retry > maxBackoffTime {
			retry = maxBackoffTime
		}

		// pause watcher routine until ctx is canceled or retry timer finishes
		r.logger.Errorf("consul service-watcher error: %v, retry in %v", err, retry)
		sleepTimer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			sleepTimer.Stop()
			r.logger.Debugf("consul service-watcher retry throttling canceled")
		case <-sleepTimer.C:
		}

		return
	}

	// reset the plan failure count
	p.failures = 0

	// if the index is unchanged do nothing
	if p.lastParamVal != nil && p.lastParamVal.equal(blockParam) {
		return
	}

	// update the plan index
	oldParamVal := p.lastParamVal
	p.lastParamVal = blockParam.next(oldParamVal)

	// compare last and new result to get changed services
	healthByService := getHealthByService(result)
	changedServices := p.getChangedServices(healthByService)

	// update the plan last result
	p.lastResult = healthByService

	// call agent to get updated healthy nodes for each changed service
	for k := range changedServices {
		p.options.WaitIndex = 0
		p.options.Filter = p.configuredQueryFilter
		p.options = p.options.WithContext(ctx)
		result, _, err := r.client.Health().Service(k, "", true, p.options)

		if err != nil {
			// on failure, expire service from cache, resolver will fall back to agent
			r.logger.Errorf("error invoking health service: %v, for service %s", err, k)
			r.registry.expire(k)

			// remove healthchecks for service from last result
			for key := range p.lastResult {
				if k == key.serviceName {
					delete(p.lastResult, key)
				}
			}

			// reset plan query index
			p.lastParamVal = waitIndexVal(0)
		} else {
			// updated service entries in registry
			r.logger.Debugf("updating registry for service:%s last-index:%s", k, strconv.FormatUint(uint64(p.lastParamVal.(waitIndexVal)), 10))
			r.registry.addOrUpdate(k, result)
		}
	}
}

func (r *resolver) runWatchLoop(p *watchPlan) {
	defer func() {
		recover()
		r.registry.removeAll()
		r.watcherStarted = false
	}()

	watchTask := make(chan bool, 1)

	for {
		ctx, cancel := context.WithCancel(context.Background())

		services := r.registry.getKeys()
		watching := false

		if len(services) > 0 {
			go r.runWatchPlan(p, services, ctx, watchTask)
			watching = true
		}

		select {
		case <-watchTask:
			cancel()

		// wait on channel for new services to track
		case service := <-r.registry.registrationChannel():
			// cancel blocking query to consul agent
			cancel()

			// generate set of keys
			serviceKeys := make(map[string]interface{})
			for i := 0; i < len(services); i++ {
				serviceKeys[services[i]] = nil
			}

			// add service if it's not in the registry
			if _, ok := serviceKeys[service]; !ok {
				r.registry.addOrUpdate(service, nil)
			}

			// check for any more new services in channel
			moreServices := true
			for moreServices {
				select {
				case service := <-r.registry.registrationChannel():
					if _, ok := serviceKeys[service]; !ok {
						r.registry.addOrUpdate(service, nil)
					}
				default:
					moreServices = false
				}
			}

			if watching {
				// ensure previous routine completed before next loop
				<-watchTask
			}

			// reset plan failure count and query index
			p.failures = 0
			p.lastParamVal = waitIndexVal(0)
		}
	}
}

func (r *resolver) startWatcher() {
	r.watcherMutex.Lock()
	defer r.watcherMutex.Unlock()

	if r.watcherStarted {
		return
	}

	options := *r.config.QueryOptions
	plan := &watchPlan{
		options:               &options,
		configuredQueryFilter: options.Filter,
		lastResult:            make(map[serviceIdentifier]bool),
	}

	r.watcherStarted = true
	go r.runWatchLoop(plan)
}
