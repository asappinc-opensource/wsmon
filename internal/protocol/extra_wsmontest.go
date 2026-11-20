//go:build wsmontest

package protocol

import (
	"encoding/json"
	"fmt"
)

// NewTestExtra builds the test-only additive field used by TC-CONTRACT-11.
// It exists only in binaries built with
// `-tags wsmontest`; production builds have no way to construct a non-zero
// Extra, and no environment variable reaches it.
func NewTestExtra(key string, value any) (Extra, error) {
	k, err := json.Marshal(key)
	if err != nil {
		return Extra{}, err
	}
	v, err := json.Marshal(value)
	if err != nil {
		return Extra{}, fmt.Errorf("protocol: test extra value: %w", err)
	}
	return Extra{kv: append(append(k, ':'), v...)}, nil
}
