// Package fault holds TC-FAULT-01…10: the in-process client
// against a `wsmon server` image (this commit by default) behind toxiproxy,
// run by `make test-fault`. Without the WSMON_FAULT_* environment the tests
// skip.
package fault
