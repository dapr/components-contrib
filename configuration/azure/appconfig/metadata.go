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

package appconfig

import "time"

type metadata struct {
	Host                  string `mapstructure:"host"`
	ConnectionString      string `mapstructure:"connectionString"`
	MaxRetries            int    `mapstructure:"maxRetries"`
	MaxRetryDelay         *int   `mapstructure:"maxRetryDelay"`
	RetryDelay            *int   `mapstructure:"retryDelay"`
	SubscribePollInterval *int   `mapstructure:"subscribePollInterval"`
	RequestTimeout        *int   `mapstructure:"requestTimeout"`

	internalRequestTimeout        time.Duration `mapstructure:"-"`
	internalMaxRetryDelay         time.Duration `mapstructure:"-"`
	internalSubscribePollInterval time.Duration `mapstructure:"-"`
	internalRetryDelay            time.Duration `mapstructure:"-"`
}
