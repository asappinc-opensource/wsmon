package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/asappinc-opensource/wsmon/internal/config"
)

func TestCLI(t *testing.T) {
	for _, tc := range []struct {
		args       []string
		code       int
		out, inErr string
	}{
		{nil, 2, "", "usage: wsmon"},
		{[]string{"bogus"}, 2, "", "usage: wsmon"},
		{[]string{"server", "extra"}, 2, "", "usage: wsmon"},
		{[]string{"help"}, 0, "usage: wsmon", ""},
		{[]string{"--help"}, 0, "usage: wsmon", ""},
		{[]string{"version"}, 0, version, ""},
		{[]string{"server", "-h"}, 0, config.EnvMaxConnections, ""},
		{[]string{"client", "--help"}, 0, config.EnvURL, ""},
	} {
		var out, errb bytes.Buffer
		code := run(tc.args, &out, &errb)
		if code != tc.code || !strings.Contains(out.String(), tc.out) || !strings.Contains(errb.String(), tc.inErr) {
			t.Errorf("wsmon %v: exit %d\nstdout %q\nstderr %q", tc.args, code, out.String(), errb.String())
		}
	}
}

// TestRoleHelpListsEveryVariable: `wsmon <role> -h` is generated from the
// same Var table the loader uses, so it cannot go stale.
func TestRoleHelpListsEveryVariable(t *testing.T) {
	for role, vars := range map[string][]config.Var{"server": config.ServerVars, "client": config.ClientVars} {
		var out bytes.Buffer
		run([]string{role, "-h"}, &out, &out)
		for _, v := range vars {
			if !strings.Contains(out.String(), v.Name) {
				t.Errorf("wsmon %s -h does not list %s", role, v.Name)
			}
		}
	}
}

func TestInvalidConfigExits2(t *testing.T) {
	t.Setenv(config.EnvLogLevel, "loud")
	for _, role := range []string{"server", "client"} {
		var out bytes.Buffer
		if code := run([]string{role}, &out, &out); code != 2 {
			t.Errorf("wsmon %s with an invalid config: exit %d, want 2", role, code)
		}
		if !strings.Contains(out.String(), `"invalid configuration"`) || !strings.Contains(out.String(), config.EnvLogLevel) {
			t.Errorf("wsmon %s: log does not name the variable:\n%s", role, out.String())
		}
	}
}
