/*
Copyright 2025 The Dapr Authors
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

/* Copied from langchaingo internal chat.go (https://github.com/tmc/langchaingo/blob/77504b877f5b7680380ec6b4c047e9cccf5726b3/llms/openai/internal/openaiclient/chat.go)
The MIT License

Copyright (c) Travis Cline <travis.cline@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package openai

// ToolType is the type of a tool.
type ToolType string

const (
	ToolTypeFunction ToolType = "function"
)

// Tool is a tool to use in a chat request.
type Tool struct {
	Type     ToolType           `json:"type"`
	Function FunctionDefinition `json:"function,omitempty"`
}

// ToolChoice is a choice of a tool to use.
type ToolChoice struct {
	Type     ToolType     `json:"type"`
	Function ToolFunction `json:"function,omitempty"`
}

// ToolFunction is a function to be called in a tool choice.
type ToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolCall is a call to a tool.
type ToolCall struct {
	ID       string       `json:"id,omitempty"`
	Type     ToolType     `json:"type"`
	Function ToolFunction `json:"function,omitempty"`
}

// FunctionDefinition is a definition of a function that can be called by the model.
type FunctionDefinition struct {
	// Name is the name of the function.
	Name string `json:"name"`
	// Description is a description of the function.
	Description string `json:"description,omitempty"`
	// Parameters is a list of parameters for the function.
	Parameters any `json:"parameters"`
	// Strict is a flag to enable structured output mode.
	Strict bool `json:"strict,omitempty"`
}
