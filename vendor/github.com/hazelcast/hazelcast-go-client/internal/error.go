// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func createHazelcastError(err *proto.ServerError) core.HazelcastError {
	stackTrace := ""
	for _, trace := range err.StackTrace() {
		stackTrace += fmt.Sprintf("\n %s.%s(%s:%d)", trace.DeclaringClass(), trace.MethodName(), trace.FileName(),
			trace.LineNumber())
	}
	message := fmt.Sprintf("got exception from server:\n %s: %s\n %s", err.ClassName(), err.Message(), stackTrace)
	switch bufutil.ErrorCode(err.ErrorCode()) {
	case bufutil.ErrorCodeAuthentication:
		return core.NewHazelcastAuthenticationError(message, err)
	case bufutil.ErrorCodeHazelcastInstanceNotActive:
		return core.NewHazelcastInstanceNotActiveError(message, err)
	case bufutil.ErrorCodeHazelcastSerialization:
		return core.NewHazelcastSerializationError(message, err)
	case bufutil.ErrorCodeTargetDisconnected:
		return core.NewHazelcastTargetDisconnectedError(message, err)
	case bufutil.ErrorCodeTargetNotMember:
		return core.NewHazelcastTargetNotMemberError(message, err)
	case bufutil.ErrorCodeUnsupportedOperation:
		return core.NewHazelcastUnsupportedOperationError(message, err)
	case bufutil.ErrorCodeConsistencyLostException:
		return core.NewHazelcastConsistencyLostError(message, err)
	case bufutil.ErrorCodeIllegalArgument:
		return core.NewHazelcastIllegalArgumentError(message, err)
	}

	return core.NewHazelcastErrorType(message, err)
}
