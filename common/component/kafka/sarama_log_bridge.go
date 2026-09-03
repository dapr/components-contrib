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
	"fmt"
	"strings"

	"github.com/dapr/kit/logger"
)

type SaramaLogBridge struct {
	daprLogger logger.Logger
}

func (b SaramaLogBridge) Print(v ...interface{}) {
	b.log(fmt.Sprint(v...))
}

func (b SaramaLogBridge) Printf(format string, v ...interface{}) {
	b.log(fmt.Sprintf(format, v...))
}

func (b SaramaLogBridge) Println(v ...interface{}) {
	b.log(fmt.Sprintln(v...))
}

// log routes Sarama's log lines to Error when they report an error, since
// Sarama's Logger interface has no level of its own and previously
// everything was logged at Debug, hiding real connectivity/consumer errors.
func (b SaramaLogBridge) log(msg string) {
	if strings.Contains(strings.ToLower(msg), "error") {
		b.daprLogger.Error(msg)
		return
	}
	b.daprLogger.Debug(msg)
}
