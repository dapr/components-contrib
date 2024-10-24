package consul

import (
	"context"
	"errors"
	"strings"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	consul "github.com/hashicorp/consul/api"
)

const (
	// initial back interval.
	initialBackOffInternal = 5 * time.Second

	// maximum back off time, this is to prevent exponential runaway.
	maxBackOffInternal = 180 * time.Second
)

// A watchPlan contains all the state tracked in the loop
// that keeps the consul service registry cache fresh
type watchPlan struct {
	expired                  bool
	lastParamVal             blockingParamVal
	lastResult               map[serviceIdentifier]bool
	options                  *consul.QueryOptions
	healthServiceQueryFilter string
	failing                  bool
	backOff                  *backoff.ExponentialBackOff
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
		// this value is the same as the previous index, reset
		return waitIndexVal(0)
	}

	return idx
}

type serviceIdentifier struct {
	serviceName string
	serviceID   string
	node        string
}

func getHealthByService(checks consul.HealthChecks) map[serviceIdentifier]bool {
	healthByService := make(map[serviceIdentifier]bool)
	for _, check := range checks {
		// generate unique identifier for service
		id := serviceIdentifier{
			serviceID:   check.ServiceID,
			serviceName: check.ServiceName,
			node:        check.Node,
		}

		// if the service is not in the map - add and init to healthy
		if state, ok := healthByService[id]; !ok {
			healthByService[id] = true
		} else if !state {
			// service exists and is already unhealthy - skip
			continue
		}

		// if the check is not healthy then set service to unhealthy
		if check.Status != consul.HealthPassing {
			healthByService[id] = false
		}
	}

	return healthByService
}

func (p *watchPlan) getChangedServices(newResult map[serviceIdentifier]bool) map[string]struct{} {
	changedServices := make(map[string]struct{}) // service name set

	// foreach new result
	for newKey, newValue := range newResult {
		// if the service exists in the old result and has the same value - skip
		if oldValue, ok := p.lastResult[newKey]; ok && newValue == oldValue {
			continue
		}

		// service is new or changed - add to set
		changedServices[newKey.serviceName] = struct{}{}
	}

	// foreach old result
	for oldKey := range p.lastResult {
		// if the service does not exist in the new result - add to set
		if _, ok := newResult[oldKey]; !ok {
			changedServices[oldKey.serviceName] = struct{}{}
		}
	}

	return changedServices
}

func getServiceNameFilter(services []string) string {
	nameFilters := make([]string, len(services))

	for i, v := range services {
		nameFilters[i] = `ServiceName=="` + v + `"`
	}

	return strings.Join(nameFilters, " or ")
}

func (r *resolver) watch(ctx context.Context, p *watchPlan, services []string) (blockingParamVal, consul.HealthChecks, error) {
	p.options = p.options.WithContext(ctx)

	if p.lastParamVal != nil {
		p.options.WaitIndex = uint64(p.lastParamVal.(waitIndexVal))
	}

	// build service name filter for all keys
	p.options.Filter = getServiceNameFilter(services)

	// request health checks for target services using blocking query
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
				return nil, nil, err
			}

			// if it failed with no wait and plan is not expired
			if p.options.WaitIndex == uint64(0) && !p.expired {
				p.lastResult = nil
				p.expired = true
				r.registry.expireAll()
			}

			return nil, nil, err
		}
	}

	p.expired = false
	return waitIndexVal(meta.LastIndex), checks, err
}

// runWatchPlan executes the following steps:
//   - requests health check changes for the target keys from the consul agent using http long polling
//   - compares the results to the previous
//   - if there is a change for a given serviceName/appId it invokes the health/service api to get a list of healthy targets
//   - signals completion of the watch plan
func (r *resolver) runWatchPlan(ctx context.Context, p *watchPlan, services []string, watchPlanComplete chan struct{}) {
	defer func() {
		// signal completion of the watch plan to unblock the watch plan loop
		watchPlanComplete <- struct{}{}
	}()

	// invoke blocking call
	blockParam, result, err := r.watch(ctx, p, services)

	// if the ctx was canceled then do nothing
	if errors.Is(err, context.Canceled) {
		return
	}

	// handle an error in the watch function
	if err != nil {
		// reset the query index so the next attempt does not
		p.lastParamVal = waitIndexVal(0)

		// perform an exponential backoff
		if !p.failing {
			p.failing = true
			p.backOff.Reset()
		}

		retry := p.backOff.NextBackOff()

		// pause watcher routine until ctx is canceled or retry timer finishes
		r.logger.Errorf("consul service-watcher error: %v, retry in %s", err, retry.Round(time.Second))
		sleepTimer := time.NewTimer(retry)
		select {
		case <-ctx.Done():
			sleepTimer.Stop()
			r.logger.Debug("consul service-watcher retry throttling canceled")
		case <-sleepTimer.C:
		}

		return
	} else {
		// reset the plan failure flag
		p.failing = false
	}

	// if the result index is unchanged do nothing
	if p.lastParamVal != nil && p.lastParamVal.equal(blockParam) {
		return
	} else {
		// update the plan index
		oldParamVal := p.lastParamVal
		p.lastParamVal = blockParam.next(oldParamVal)
	}

	// compare last and new result to get changed services
	healthByService := getHealthByService(result)
	changedServices := p.getChangedServices(healthByService)

	// update the plan last result
	p.lastResult = healthByService

	// call agent to get updated healthy nodes for each changed service
	for k := range changedServices {
		p.options.WaitIndex = 0
		p.options.Filter = p.healthServiceQueryFilter
		p.options = p.options.WithContext(ctx)
		result, meta, err := r.client.Health().Service(k, "", true, p.options)

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
			r.logger.Debugf("updating consul nr registry for service:%s last-index:%d", k, meta.LastIndex)
			r.registry.addOrUpdate(k, result)
		}
	}
}

// runWatchLoop executes the following steps in a forever loop:
//   - gets the keys from the registry
//   - executes the watch plan with the targets keys
//   - waits for (the watch plan to signal completion) or (the resolver to register a new key)
func (r *resolver) runWatchLoop(p *watchPlan) {
	defer func() {
		r.registry.removeAll()
		r.watcherStarted.Store(false)
	}()

	watchPlanComplete := make(chan struct{}, 1)

watchLoop:
	for {
		ctx, cancel := context.WithCancel(context.Background())

		// get target keys/app-ids from registry
		services := r.registry.getKeys()
		watching := false

		if len(services) > 0 {
			// run watch plan for targets service with channel to signal completion
			go r.runWatchPlan(ctx, p, services, watchPlanComplete)
			watching = true
		}

		select {
		case <-watchPlanComplete:
			cancel()

		// wait on channel for new services to track
		case service := <-r.registry.registrationChannel():
			// cancel watch plan i.e. blocking query to consul agent
			cancel()

			// generate set of keys
			serviceKeys := make(map[string]any)
			for i := range services {
				serviceKeys[services[i]] = nil
			}

			// add service if it's not in the registry
			if _, ok := serviceKeys[service]; !ok {
				r.registry.addOrUpdate(service, nil)
			}

			// check for any more new services in channel and do the same
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
				// ensure previous watch plan routine completed before next iteration
				<-watchPlanComplete
			}

			// reset plan failure count and query index
			p.failing = false
			p.lastParamVal = waitIndexVal(0)

		// resolver closing
		case <-r.watcherStopChannel:
			cancel()
			break watchLoop
		}
	}
}

// startWatcher will configure the watch plan and start the watch loop in a separate routine
func (r *resolver) startWatcher() {
	if !r.watcherStarted.CompareAndSwap(false, true) {
		return
	}

	options := *r.config.QueryOptions
	options.UseCache = false // always ignore consul agent cache for watcher
	options.Filter = ""      // don't use configured filter for State() calls

	// Configure exponential backoff
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = initialBackOffInternal
	ebo.MaxInterval = maxBackOffInternal
	ebo.MaxElapsedTime = 0

	plan := &watchPlan{
		options:                  &options,
		healthServiceQueryFilter: r.config.QueryOptions.Filter,
		lastResult:               make(map[serviceIdentifier]bool),
		backOff:                  ebo,
	}

	go r.runWatchLoop(plan)
}
