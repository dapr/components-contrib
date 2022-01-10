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

package influx

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"Url": "a", "Token": "a", "Org": "a", "Bucket": "a"}
	influx := Influx{logger: logger.NewLogger("test")}
	im, err := influx.getInfluxMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "a", im.URL)
	assert.Equal(t, "a", im.Token)
	assert.Equal(t, "a", im.Org)
	assert.Equal(t, "a", im.Bucket)
}
