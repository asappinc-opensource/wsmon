package protocol

import (
	"errors"
	"strings"
)

// IsLowerHex reports whether s is exactly n lowercase hex characters.
func IsLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// FormatTraceparent builds a W3C version-00 traceparent. The flags are "01"
// when sampled and "00" otherwise: the probe's own head-sampling decision,
// which the server honours.
func FormatTraceparent(traceID, spanID string, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return "00-" + traceID + "-" + spanID + "-" + flags
}

var errTraceparent = errors.New("protocol: invalid traceparent")

// ParseTraceparent strictly parses a W3C version-00 traceparent: exactly
// 55 bytes, lowercase hex only, non-zero trace and span IDs. sampled is bit 0
// of the flags.
func ParseTraceparent(s string) (traceID, spanID string, sampled bool, err error) {
	if len(s) != 55 || s[:3] != "00-" || s[35] != '-' || s[52] != '-' {
		return "", "", false, errTraceparent
	}
	traceID, spanID, flags := s[3:35], s[36:52], s[53:55]
	if !IsLowerHex(traceID, 32) || !IsLowerHex(spanID, 16) || !IsLowerHex(flags, 2) {
		return "", "", false, errTraceparent
	}
	if traceID == strings.Repeat("0", 32) || spanID == strings.Repeat("0", 16) {
		return "", "", false, errTraceparent
	}
	return traceID, spanID, fromHex(flags[1])&1 == 1, nil
}

func fromHex(c byte) byte {
	if c >= 'a' {
		return c - 'a' + 10
	}
	return c - '0'
}

// ValidULID reports whether s is a canonical ULID: 26 uppercase Crockford
// base32 characters whose first character is at most '7' (so the 128-bit
// value does not overflow).
func ValidULID(s string) bool {
	if len(s) != 26 || s[0] > '7' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(crockford, rune(s[i])) {
			return false
		}
	}
	return true
}

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
