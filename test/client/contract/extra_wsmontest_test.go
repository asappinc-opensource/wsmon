//go:build wsmontest

package contract

import "github.com/asappinc-opensource/wsmon/internal/protocol"

// testExtra is the additive unknown field of TC-CONTRACT-11. It exists only
// under -tags wsmontest, which `make test-contract` sets.
func testExtra() (protocol.Extra, bool) {
	x, err := protocol.NewTestExtra("wsmon_test_unknown", "x")
	return x, err == nil
}
