//go:build !wsmontest

package contract

import "github.com/asappinc-opensource/wsmon/internal/protocol"

// testExtra is unavailable without -tags wsmontest; TC-CONTRACT-11 then
// fails rather than silently passing.
func testExtra() (protocol.Extra, bool) { return protocol.Extra{}, false }
