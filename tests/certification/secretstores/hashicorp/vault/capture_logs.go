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

package vault_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

//
// Helper functions for asserting error messages during component initialization
//
// These can be exported to their own module.
// Do notice that they have side effects: using more than one in a single
// flow will cause only the latest to work. Perhaps this functionality
// (dapr.runtime log capture) could be baked into flows themselves?
//
// Also: this is not thread-safe nor concurrent safe: only one test
// can be run at a time to ensure deterministic capture of dapr.runtime output.

type InitErrorChecker func(ctx flow.Context, errorLine string) error

func CaptureLogsAndCheckInitErrors(checker InitErrorChecker) flow.Runnable {
	// Setup log capture
	logCaptor := &bytes.Buffer{}
	runtimeLogger := logger.NewLogger("dapr.runtime")
	runtimeLogger.SetOutput(io.MultiWriter(os.Stdout, logCaptor))

	// Stop log capture, reset buffer just for good mesure
	cleanup := func() {
		logCaptor.Reset()
		runtimeLogger.SetOutput(os.Stdout)
	}

	grepInitErrorFromLogs := func() (string, error) {
		errorMarker := []byte("INIT_COMPONENT_FAILURE")
		scanner := bufio.NewScanner(logCaptor)
		for scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return "", err
			}
			if bytes.Contains(scanner.Bytes(), errorMarker) {
				return scanner.Text(), nil
			}
		}
		return "", scanner.Err()
	}

	// Wraps our InitErrorChecker with cleanup and error-grepping logic so we only care about the
	// log error
	return func(ctx flow.Context) error {
		defer cleanup()

		errorLine, err := grepInitErrorFromLogs()
		if err != nil {
			return err
		}
		ctx.Logf("captured errorLine: %s", errorLine)

		return checker(ctx, errorLine)
	}
}

func AssertNoInitializationErrorsForComponent(componentName string) flow.Runnable {
	checker := func(ctx flow.Context, errorLine string) error {
		componentFailedToInitialize := strings.Contains(errorLine, componentName)
		assert.False(ctx.T, componentFailedToInitialize,
			"Found component name mentioned in an component initialization error message: %s", errorLine)

		return nil
	}

	return CaptureLogsAndCheckInitErrors(checker)
}

func AssertInitializationFailedWithErrorsForComponent(componentName string, additionalSubStringsToMatch ...string) flow.Runnable {
	checker := func(ctx flow.Context, errorLine string) error {
		assert.NotEmpty(ctx.T, errorLine, "Expected a component initialization error message but none found")
		assert.Contains(ctx.T, errorLine, componentName,
			"Expected to find component '%s' mentioned in error message but found none: %s", componentName, errorLine)

		for _, subString := range additionalSubStringsToMatch {
			assert.Contains(ctx.T, errorLine, subString,
				"Expected to find '%s' mentioned in error message but found none: %s", componentName, errorLine)
		}

		return nil
	}

	return CaptureLogsAndCheckInitErrors(checker)
}
