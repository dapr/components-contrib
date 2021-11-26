// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package local

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	ps "github.com/mitchellh/go-ps"
	"github.com/pkg/errors"
	process "github.com/shirou/gopsutil/process"
)

type resolver struct {
	logger logger.Logger
}

func NewResolver(logger logger.Logger) nameresolution.Resolver {
	return &resolver{
		logger: logger,
	}
}

func (resolver *resolver) Init(metadata nameresolution.Metadata) error {
	return nil
}

func (resolver *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	processes, err := ps.Processes()
	if err != nil {
		return "", fmt.Errorf("error iterating through processes: %s", err)
	}

	for _, proc := range processes {
		executable := strings.ToLower(proc.Executable())
		if executable == "daprd" || executable == "daprd.exe" {
			procDetails, err := process.NewProcess(int32(proc.Pid()))
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

			if req.ID != appID {
				continue
			}

			grpcPortStr := "50001"
			if v, ok := argumentsMap["--dapr-internal-grpc-port"]; ok {
				grpcPortStr = v
			}

			grpcPort, err := strconv.Atoi(grpcPortStr)

			if err != nil {
				return "", err
			}

			return fmt.Sprintf("127.0.0.1:%d", grpcPort), nil
		}
	}
	return "", errors.Errorf("coud not find service %s", req.ID)
}
