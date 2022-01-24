/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package local

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/go-ps"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/process"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

type procInfo struct {
	processID int32
	grpcPort  int
}

type resolver struct {
	logger           logger.Logger
	processCache     map[string]*procInfo
	processCacheLock *sync.Mutex
	ticker           *time.Ticker
}

func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger:           logger,
		processCache:     make(map[string]*procInfo),
		processCacheLock: &sync.Mutex{},
		ticker:           time.NewTicker(time.Second * 30),
	}
}

func (resolver *resolver) scheduleCacheRefresh() {
	for range resolver.ticker.C {
		resolver.refreshCache()
	}
}

func (resolver *resolver) refreshCache() {
	resolver.processCacheLock.Lock()
	defer resolver.processCacheLock.Unlock()

	processes, err := ps.Processes()
	if err != nil {
		resolver.logger.Errorf("error iterating through processes: %s", err)
		return
	}

	for k := range resolver.processCache {
		delete(resolver.processCache, k)
	}

	for _, proc := range processes {
		executable := strings.ToLower(proc.Executable())
		if executable == "daprd" || executable == "daprd.exe" {
			procID := int32(proc.Pid())
			procDetails, err := process.NewProcess(procID)
			if err != nil {
				continue
			}

			cmdLine, err := procDetails.Cmdline()
			if err != nil {
				continue
			}

			cmdLineItems := strings.Fields(cmdLine)
			if len(cmdLineItems) <= 1 {
				continue
			}

			argumentsMap := make(map[string]string)
			for i := 1; i < len(cmdLineItems)-1; i += 2 {
				argumentsMap[cmdLineItems[i]] = cmdLineItems[i+1]
			}

			appID := argumentsMap["--app-id"]

			grpcPortStr := "50001"
			if v, ok := argumentsMap["--dapr-internal-grpc-port"]; ok {
				grpcPortStr = v
			}

			grpcPort, err := strconv.Atoi(grpcPortStr)
			if err != nil {
				continue
			}
			resolver.logger.Debugf("updating app %s grpcPort %d", appID, grpcPort)

			resolver.processCache[appID] = &procInfo{
				processID: procID,
				grpcPort:  grpcPort,
			}
		}
	}
}

func (resolver *resolver) Init(metadata nameresolution.Metadata) error {
	go resolver.scheduleCacheRefresh()
	return nil
}

func (resolver *resolver) getValue(id string) (*procInfo, bool) {
	resolver.processCacheLock.Lock()
	defer resolver.processCacheLock.Unlock()
	procInfo, ok := resolver.processCache[id]
	return procInfo, ok
}

func (resolver *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	procInfo, ok := resolver.getValue(req.ID)

	if !ok {
		// attempt to refresh the cache once in case req.ID just popped up
		resolver.refreshCache()
		procInfo, ok = resolver.getValue(req.ID)
		if !ok {
			return "", errors.Errorf("coud not find service %s", req.ID)
		}
	}

	procDetails, err := process.NewProcess(procInfo.processID)
	if err != nil {
		return "", errors.Errorf("coud not find service %s", req.ID)
	}

	name, err := procDetails.Name()
	if err != nil {
		return "", errors.Errorf("coud not find service %s", req.ID)
	}

	if !strings.Contains(name, "daprd") {
		return "", errors.Errorf("could not find service %s", req.ID)
	}

	return fmt.Sprintf("127.0.0.1:%d", procInfo.grpcPort), nil
}
