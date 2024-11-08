/*
Copyright 2024 The Dapr Authors
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
package conversation

import (
	"context"
	"fmt"
	"time"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/cache"
	"github.com/tmc/langchaingo/llms/cache/inmemory"
)

// LangchainTemperature returns a langchain compliant LLM temperature
func LangchainTemperature(temperature float64) llms.CallOption {
	return llms.WithTemperature(temperature)
}

// CacheModel creates a prompt query cache with a configured TTL
func CacheModel(ctx context.Context, ttl string, model llms.Model) (llms.Model, error) {
	d, err := time.ParseDuration(ttl)
	if err != nil {
		return model, fmt.Errorf("failed to parse cacheTTL duration: %s", err)
	}

	mem, err := inmemory.New(ctx, inmemory.WithExpiration(d))
	if err != nil {
		return model, fmt.Errorf("failed to create llm cache: %s", err)
	}

	return cache.New(model, mem), nil
}
