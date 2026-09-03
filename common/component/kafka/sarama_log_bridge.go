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

	"github.com/dapr/kit/logger"
)

// SaramaLogBridge implements sarama.StdLogger over the Dapr logger. Sarama
// hands over pre-rendered strings, so records carry no structured attributes;
// everything is emitted at debug level as before.
type SaramaLogBridge struct {
	log *logger.Log
}

func newSaramaLogBridge(l logger.Logger) SaramaLogBridge {
	return SaramaLogBridge{log: logger.FromLogger(l)}
}

func (b SaramaLogBridge) Print(v ...interface{}) {
	b.log.Debug(fmt.Sprint(v...))
}

func (b SaramaLogBridge) Printf(format string, v ...interface{}) {
	b.log.Debug(fmt.Sprintf(format, v...))
}

func (b SaramaLogBridge) Println(v ...interface{}) {
	// The previous bridge rendered Println identically to Print, and output
	// compatibility wins over stdlib Println semantics here.
	b.log.Debug(fmt.Sprint(v...))
}
