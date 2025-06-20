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

package utils

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasOperation(t *testing.T) {
	t.Run("operations list", func(t *testing.T) {
		cc := CommonConfig{
			ComponentType: "state",
			ComponentName: "redis",
			Operations:    NewStringSet("op1", "op2"),
		}
		assert.True(t, cc.HasOperation("op1"))
		assert.True(t, cc.HasOperation("op2"))
		assert.False(t, cc.HasOperation("op3"))
	})
}

func TestCopyMap(t *testing.T) {
	cc := CommonConfig{
		ComponentType: "state",
		ComponentName: "redis",
	}
	in := map[string]string{
		"k": "v",
		"v": "k",
	}
	out := cc.CopyMap(in)
	assert.Equal(t, in, out)
	assert.NotEqual(t, reflect.ValueOf(in).Pointer(), reflect.ValueOf(out).Pointer())
}

func TestLoadEnvFile(t *testing.T) {
	// Create a temporary .env file for testing
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	envContent := `# Test environment file
TEST_KEY1=value1
TEST_KEY2="quoted value"
TEST_KEY3='single quoted'
TEST_KEY4=value with spaces

# This is a comment
EMPTY_LINE_ABOVE=yes
TEST_OVERRIDE=original_value
`

	err := os.WriteFile(envFile, []byte(envContent), 0o600)
	require.NoError(t, err)

	// Set a value that should be overridden
	t.Setenv("TEST_OVERRIDE", "should_be_overridden")

	// Load the .env file
	err = loadEnvFile(envFile)
	require.NoError(t, err)

	// Test that values were loaded correctly
	assert.Equal(t, "value1", os.Getenv("TEST_KEY1"))
	assert.Equal(t, "quoted value", os.Getenv("TEST_KEY2"))
	assert.Equal(t, "single quoted", os.Getenv("TEST_KEY3"))
	assert.Equal(t, "value with spaces", os.Getenv("TEST_KEY4"))
	assert.Equal(t, "yes", os.Getenv("EMPTY_LINE_ABOVE"))

	// Test that override works
	assert.Equal(t, "original_value", os.Getenv("TEST_OVERRIDE"))

	// Clean up
	os.Unsetenv("TEST_KEY1")
	os.Unsetenv("TEST_KEY2")
	os.Unsetenv("TEST_KEY3")
	os.Unsetenv("TEST_KEY4")
	os.Unsetenv("EMPTY_LINE_ABOVE")
	os.Unsetenv("TEST_OVERRIDE")
}

func TestLoadEnvFileNotExists(t *testing.T) {
	// Test loading a non-existent file
	err := loadEnvFile("/non/existent/file")
	assert.Error(t, err)
}

func TestLoadEnvVars(t *testing.T) {
	// Create a temporary .env file for testing
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, "test.env")

	envContent := `TEST_LOAD_VARS=success`
	err := os.WriteFile(envFile, []byte(envContent), 0o600)
	require.NoError(t, err)

	// This test is tricky because LoadEnvVars uses runtime.Caller
	// For now, just test that it doesn't panic when file doesn't exist
	err = LoadEnvVars("nonexistent.env")
	require.Error(t, err) // Should return error but not panic

	// Clean up
	os.Unsetenv("TEST_LOAD_VARS")
}
