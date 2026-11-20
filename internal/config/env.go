// Package config loads the WSMON_* environment for both roles of the wsmon
// binary. The two roles share one loader, one set of parsing and validation
// rules, one redaction policy and one definition of every variable that means
// the same thing on both sides (WSMON_AUTH_TOKEN, WSMON_METRICS_ADDR,
// WSMON_OTLP_ENDPOINT, ...). Limits that the server enforces and the client
// must respect come from internal/protocol, not from literals here.
//
//	env.go     the generic loader, Var tables, errors and redaction
//	shared.go  variables and rules common to both roles
//	server.go  `wsmon server`
//	client.go  `wsmon client`
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"
)

// Var documents one environment variable: its name, its default ("" for
// required variables and empty-string defaults), whether it is a secret, and
// a one-line description for `wsmon <role> -h`.
type Var struct {
	Name    string
	Default string
	Secret  bool
	Help    string
}

// Error carries every configuration problem found. Each problem names the
// offending variable and never contains a secret value.
type Error struct{ Problems []string }

func (e *Error) Error() string { return "invalid configuration: " + strings.Join(e.Problems, "; ") }

// IsError reports whether err is a configuration *Error and returns its
// problems.
func IsError(err error) ([]string, bool) {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Problems, true
	}
	return nil, false
}

// loader reads variables against one role's Var table. An unset and an empty
// variable are the same: the default applies. Every problem is collected so
// the operator sees all of them in one log line.
type loader struct {
	getenv   func(string) string
	vars     map[string]Var
	problems []string
}

func newLoader(getenv func(string) string, vars []Var) *loader {
	m := make(map[string]Var, len(vars))
	for _, v := range vars {
		m[v.Name] = v
	}
	return &loader{getenv: getenv, vars: m}
}

func (l *loader) addf(name, format string, args ...any) {
	l.problems = append(l.problems, name+": "+fmt.Sprintf(format, args...))
}

func (l *loader) err() error {
	if len(l.problems) == 0 {
		return nil
	}
	return &Error{Problems: l.problems}
}

// raw returns the variable's value, or its default when unset or empty.
func (l *loader) raw(name string) string {
	if _, ok := l.vars[name]; !ok {
		panic("config: variable " + name + " is not in this role's Var table")
	}
	if v := strings.TrimSpace(l.getenv(name)); v != "" {
		return v
	}
	return l.vars[name].Default
}

func (l *loader) str(name string) string { return l.raw(name) }

// secret returns a secret verbatim (no trimming: a secret's bytes are its
// own business). It is never echoed into a problem message.
func (l *loader) secret(name string) string { return l.getenv(name) }

// duration parses a duration that must be > 0.
func (l *loader) duration(name string) time.Duration { return l.durationAbove(name, 0, false) }

// durationMin0 parses a duration that must be >= 0.
func (l *loader) durationMin0(name string) time.Duration { return l.durationAbove(name, 0, true) }

func (l *loader) durationAbove(name string, floor time.Duration, inclusive bool) time.Duration {
	s := l.raw(name)
	d, err := time.ParseDuration(s)
	switch {
	case err != nil:
		l.addf(name, "invalid duration %q", s)
		return 0
	case inclusive && d < floor:
		l.addf(name, "must be >= %s, got %s", floor, s)
	case !inclusive && d <= floor:
		l.addf(name, "must be > %s, got %s", floor, s)
	}
	return d
}

func (l *loader) integer(name string) int64 {
	n, _ := l.parseInt(name)
	return n
}

func (l *loader) parseInt(name string) (int64, bool) {
	s := l.raw(name)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		l.addf(name, "invalid integer %q", s)
		return 0, false
	}
	return n, true
}

// positive parses an integer that must be > 0.
func (l *loader) positive(name string) int64 {
	n, ok := l.parseInt(name)
	if ok && n <= 0 {
		l.addf(name, "must be > 0, got %d", n)
	}
	return n
}

func (l *loader) ratio(name string) float64 {
	s := l.raw(name)
	r, err := strconv.ParseFloat(s, 64)
	switch {
	case err != nil:
		l.addf(name, "invalid number %q", s)
	case math.IsNaN(r) || r < 0 || r > 1:
		l.addf(name, "must be within [0,1], got %s", s)
	}
	return r
}

func (l *loader) boolean(name string) bool {
	s := l.raw(name)
	b, err := strconv.ParseBool(s)
	if err != nil {
		l.addf(name, "invalid boolean %q", s)
	}
	return b
}

func (l *loader) csv(name string) []string {
	s := l.raw(name)
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			l.addf(name, "empty item at position %d in %q", i+1, s)
			return nil
		}
		out = append(out, p)
	}
	return out
}

func (l *loader) logLevel(name string) slog.Level {
	switch s := strings.ToLower(l.raw(name)); s {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		l.addf(name, "must be one of debug, info, warn, error; got %q", s)
		return slog.LevelInfo
	}
}

const redacted = "[redacted]"

// kv is one variable's resolved value, rendered as a string.
type kv struct{ name, value string }

// redactedAttrs renders resolved values as log attributes keyed by env name.
// Secrets are "[redacted]" when set and "" when unset; nothing about a
// secret's length or content is revealed.
func redactedAttrs(vars []Var, values []kv) []slog.Attr {
	secret := map[string]bool{}
	for _, v := range vars {
		secret[v.Name] = v.Secret
	}
	out := make([]slog.Attr, 0, len(values))
	for _, v := range values {
		if secret[v.name] && v.value != "" {
			v.value = redacted
		}
		out = append(out, slog.String(v.name, v.value))
	}
	return out
}

// hashInput is a canonical serialisation of the resolved values, secrets
// excluded. Two configs that resolve to the same values hash the same
// regardless of how they were spelled ("1000ms" vs "1s"). WSMON_NAME is
// excluded too: it names the process, so replicas of one configuration (and
// restarts that default it to a new host name) still hash the same.
func hashInput(vars []Var, values []kv) []byte {
	secret := map[string]bool{}
	for _, v := range vars {
		secret[v.Name] = v.Secret
	}
	var b strings.Builder
	for _, v := range values {
		if secret[v.name] || v.name == EnvName {
			continue
		}
		b.WriteString(v.name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(v.value))
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// Usage renders a Var table for `wsmon <role> -h`.
func Usage(vars []Var) string {
	w := 0
	for _, v := range vars {
		w = max(w, len(v.Name))
	}
	var b strings.Builder
	for _, v := range vars {
		fmt.Fprintf(&b, "  %-*s  %s", w, v.Name, v.Help)
		switch {
		case v.Default != "":
			fmt.Fprintf(&b, " (default %s)", v.Default)
		case !strings.HasPrefix(v.Help, "required"):
			b.WriteString(` (default "")`)
		}
		b.WriteByte('\n')
	}
	return b.String()
}
