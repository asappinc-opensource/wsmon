# Golden frames

## Client → server: byte-exact encoder output (TC-WIRE-19)

The client encoder in `encode_client.go` must reproduce these files **byte for byte**. They
have no trailing newline. The inputs that produce them are in the
`clientGoldens` table in `golden_test.go`; change both together.

| File | Source |
|---|---|
| `probe.json` | Example probe, `small` payload: no `padding` key. `client_send_epoch_ns` is followed by one space, the rest of its 20-byte slot. |
| `probe_large.json` | Same inputs as `probe.json`, padded with `x` to exactly 32768 bytes. |
| `hello.json` | Example hello, `pingpong` mode, `push_interval_ms: 1000` (non-default options). |
| `hello_idle.json` | Example hello (`idle` mode, `server_keepalive: false`, `push_interval_ms: 0`). |
| `push_ack.json` | Example `push_ack`. |

## Server → client: spec-derived (decode tests)

Pretty-printed. Decoded with a real JSON decoder, never byte-compared.

| File | Source |
|---|---|
| `echo.json` | Example echo. |
| `push.json` | Example push, elided values completed. |
| `hello_ack.json` | Example `hello_ack`, elided values completed. |
| `error.json` | Example error. |
| `error_<code>.json` | One per error code. Messages as `wsmon server` sends them. |
| `echo_patched.json` | Compact; exactly the layout of `AppendEcho` in `codec_server.go`, with `server_send_epoch_ns` followed by spaces to fill its 20-byte slot. |
| `echo_unknown_fields.json` | Example echo values with unknown top-level and nested keys and extra whitespace everywhere (TC-WIRE-20). |

Elided values in the spec examples (`"9c1e..."`, `"01J9ZQ8K..."`, `"..."`) are
completed with valid values of the right shape.

Beyond these files, `roundtrip_test.go` feeds every encoder's output to the
other role's decoder, so the two halves of the contract are checked against
each other directly rather than through fixtures.

## Captured frames

`captured/` (gitignored) is written by TC-CONTRACT-01 from the frames the
server under test actually sent.
