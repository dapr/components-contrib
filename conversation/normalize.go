package conversation

import (
	"os"
	"strings"
)

// Normalized finish reasons - standardized across all providers
const (
	FinishReasonStop          = "stop"           // Natural completion or stop sequence reached
	FinishReasonLength        = "length"         // Token limit reached (max_tokens)
	FinishReasonToolCalls     = "tool_calls"     // Tool/function calling initiated
	FinishReasonContentFilter = "content_filter" // Response blocked for safety/content reasons
	FinishReasonError         = "error"          // Error occurred during generation
	FinishReasonUnknown       = "unknown"        // Unknown or unspecified reason
)

// NormalizeFinishReason normalizes provider-specific finish reasons to standard values
// This ensures consistent finish reason values across all conversation components
func NormalizeFinishReason(providerReason string) string {
	// Normalize to lowercase for comparison
	reason := strings.ToLower(strings.TrimSpace(providerReason))

	switch reason {
	// Standard stop reasons
	case "stop", "end_turn", "eos_token", "stop_sequence", "finish":
		return FinishReasonStop

	// Length/token limit reasons
	case "length", "max_tokens", "max_output_tokens":
		return FinishReasonLength

	// Tool calling reasons
	case "tool_calls", "tool_use", "function_call":
		return FinishReasonToolCalls

	// Safety/content filter reasons
	case "safety", "content_filter", "content_filtered", "prohibited_content", "spii":
		return FinishReasonContentFilter

	// Error conditions
	case "error", "insufficient_system_resource", "recitation":
		return FinishReasonError

	// Empty or unspecified
	case "", "finish_reason_unspecified", "blocked_reason_unspecified", "other":
		return FinishReasonUnknown

	default:
		// Return the original reason if we don't have a mapping
		// This allows for provider-specific reasons while still providing normalization
		return providerReason
	}
}

// GetEnvKey returns the first non-empty environment variable from the provided options.
// This is a fallback when the provider component does not have a key. This is good for development.
func GetEnvKey(options ...string) string {
	for _, option := range options {
		if os.Getenv(option) != "" {
			return option
		}
	}
	return ""
}
