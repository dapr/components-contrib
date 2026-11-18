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

package kafka

import (
	"errors"

	"github.com/IBM/sarama"

	"github.com/dapr/kit/logger"
)

type SaramaLogBridge struct {
	daprLogger logger.Logger
}

// log sends consumer errors (which Sarama only reports through its Logger when
// Consumer.Return.Errors is false) at error level and everything else at debug.
func (b SaramaLogBridge) log(v ...interface{}) {
	for _, a := range v {
		if err, ok := a.(error); ok {
			var cerr *sarama.ConsumerError
			if errors.As(err, &cerr) {
				b.daprLogger.Error(v...)
				return
			}
		}
	}
	b.daprLogger.Debug(v...)
}

func (b SaramaLogBridge) Print(v ...interface{}) {
	b.log(v...)
}

func (b SaramaLogBridge) Printf(format string, v ...interface{}) {
	b.daprLogger.Debugf(format, v...)
}

func (b SaramaLogBridge) Println(v ...interface{}) {
	b.log(v...)
}
