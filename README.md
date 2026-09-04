# Sigil Parquet plugin

`wasm.parquet` is a bounded, read-only Parquet inspector and typed scalar,
column, or projected-row reader for Sigil scenarios. Public stable 0.2.0 is an
immutable keyless-provenance release. It accepts complete Parquet file bytes,
reports flat leaf-column metadata, reads one cell, reads one exact column
window, or reads an exact row window for an ordered projection.

It composes directly with byte-producing plugins such as `wasm.s3`:

```lua
local s3 = require("wasm.s3")
local parquet = require("wasm.parquet")

local bytes, get_err = s3["get-object"]({
  bucket = "results",
  key = "run/output.parquet",
  auth = { tag = "sigv4", value = "object-store-read" },
  ["max-bytes"] = 4 * 1024 * 1024,
})
expect(bytes ~= nil, get_err and get_err.message)

local cell, read_err = parquet["read-cell"](bytes, {
  column = "total",
  row = 1,
})
expect(cell ~= nil, read_err and read_err.message)
expect(cell.tag == "floating")
expect(cell.value == 27.75)
```

The 0.1.1 release adds a typed column window without reparsing the file or
redecoding a page for every cell:

```lua
local cells, column_err = parquet["read-column"](bytes, {
  column = "event_epoch",
  offset = 0,
  limit = 100,
})
expect(cells ~= nil, column_err and column_err.message)
for _, cell in ipairs(cells) do
  expect(cell.tag == "signed")
  expect(cell.value >= 1_000_000_000 and cell.value <= 9_999_999_999)
end
```

The 0.2.0 release makes a breaking WIT shape correction: Parquet's
`isAdjustedToUTC` flag is now preserved as `is-adjusted-to-utc`. Structured
column metadata returns `true` or `false` for every logical or deprecated
converted TIME/TIMESTAMP annotation, and `nil` for non-temporal columns. Every
time or timestamp cell carries the same required boolean beside its unit and
raw integer value. In particular, `false` is data rather than absence:

```lua
local info, inspect_err = parquet.inspect(bytes)
expect(info ~= nil, inspect_err and inspect_err.message)
expect(info.columns[1].logical_type == "timestamp(milliseconds)")
expect(info.columns[1]["is-adjusted-to-utc"] == false)

local cell, read_err = parquet["read-cell"](bytes, {
  column = "observed_at",
  row = 0,
})
expect(cell ~= nil, read_err and read_err.message)
expect(cell.tag == "timestamp")
expect(cell.value.unit == "milliseconds")
expect(cell.value["is-adjusted-to-utc"] == false)
```

The existing `logical_type` string is intentionally unchanged so callers can
migrate by reading the new structured field. When a file has only a deprecated
`TIME_MILLIS`, `TIME_MICROS`, `TIMESTAMP_MILLIS`, or `TIMESTAMP_MICROS`
ConvertedType annotation, 0.2.0 maps it explicitly to UTC-adjusted `true`, as
specified by the Parquet backward-compatibility table. It never guesses from
the integer value or time unit. If both representations are present, the
parameterized LogicalType is authoritative, so local (`false`) semantics are
not overwritten by its compatibility ConvertedType.

| Public 0.1.0 | Public 0.1.1 | Public stable 0.2.0 |
|---|---|---|
| `inspect` and `read-cell` | preserves both and adds `read-column` plus projected `read-rows` | preserves all four operations |
| one cell per decode call | one exact typed window per call, parsing the file once | adds explicit UTC-adjustment semantics to temporal metadata and cells |
| fixed 16 MiB complete-file cap | the same fixed cap | the same fixed cap; no ambient or invented network grant |

Version 0.2.0 requires Sigil 0.31.0 or newer and is available as an immutable
keyless-provenance package. Add the exact public identity with
`sigil plugin add parquet@0.2.0`.

The same release can decode a small comparison matrix in one call. Column
identity is returned once in `batch.columns`; every positional row cell uses
that exact order:

```lua
local batch, rows_err = parquet["read-rows"](bytes, {
  columns = { "order_id", "status", "event_epoch" },
  offset = 0,
  limit = 3,
})
expect(batch ~= nil, rows_err and rows_err.message)
expect(batch.columns[1] == "order_id")
expect(batch.columns[2] == "status")
expect(batch.columns[3] == "event_epoch")
expect(#batch.rows == 3)
expect(batch.rows[1].cells[1].tag == "signed")
```

Windows are exact: `offset == rows, limit == 0` succeeds, while any nonempty
window beyond the end fails as `not-found`; a resource bound fails as `limit`
and never returns a short result. A row projection must be nonempty and ordered;
duplicate, unknown, nested, repeated, or unsupported columns fail before page
decode. The decoder resolves every selected flat column first, skips unrelated
columns, decodes each selected overlapping page at most once, and performs one
linear column-to-row transpose. It preserves the existing tagged NULL, DECIMAL,
and timestamp values without normalising decimal scale, temporal units, or
UTC-adjustment semantics.

The composition example above uses the public stable S3 0.3.0 tagged-auth API;
Parquet 0.2.0 accepts its byte-exact output. The complete pairing requires
stable Sigil 0.33.2 or newer because S3 0.3.0 requires Host API 1.2; Parquet
itself still requires only Sigil 0.31.0 or newer and Host API 1.0. Add both
exact project locks before evaluating that example:

```bash
sigil plugin add s3@0.3.0
sigil plugin add parquet@0.2.0
```

The plugin imports no host capability. The calling scenario still declares the
capabilities inferred from its `require` calls, including `wasm.s3` and
`wasm.parquet`; only S3 needs a network grant.

The initial decoder supports required or optional, non-repeated scalar columns,
plain and dictionary encoding, uncompressed pages, and Snappy compression. It
preserves booleans, signed and unsigned integers, floats, UTF-8 text, bytes,
decimals, dates, and millisecond or microsecond time/timestamp values. Nested
or repeated columns, INT96, nanosecond temporal values, UUID/interval values,
external column chunks, other encodings, and other compression codecs fail
explicitly as unsupported.

Bounds are part of the contract: 16 MiB input, one million rows, 4,096 row
groups, 1,024 leaf columns, 1 KiB column paths, 1 MiB pages and decoded cell
values, and 32 MiB uncompressed column chunks. A `read-column` result is also
limited to 100,000 cells and 16 MiB of accounted decoded output. A `read-rows`
result is limited to 128 selected columns, 10,000 rows, 100,000 cells, 8 MiB of
structural assembly, and 16 MiB including cell payloads. Counts, multiplication,
addition, allocation, and aggregate bytes are checked before a value is
exposed. Parser and codec errors are collapsed to stable messages rather than
exposing input-derived details.

The 16 MiB complete-file input cap deliberately remains fixed in 0.2.0. This
pure plugin has no route or grant from which to derive a network-style byte
allowance; larger complete files or range-backed reads need a Sigil-owned
resource-limit design rather than an invented Parquet network grant.

Install the official immutable 0.2.0 release and add it to the current project:

```bash
sigil plugin install parquet@0.2.0
sigil plugin add parquet@0.2.0
```

Build, test the compression and temporal fixtures, validate the component, and
pack a local development archive:

```bash
just check
just dist
```

Use a locally built archive only in an isolated development cache; local-path
packages are cache-only and cannot authorize a project lock:

```bash
sigil plugin install --path dist/parquet-0.2.0.sigil-plugin.tar.zst
```

With a Sigil build that provides the non-gating local candidate command, run
the checked-in Lua/WIT shape test through the production bridge:

```bash
just candidate-check
```

That check feeds a deterministic file generated by the pinned Apache Parquet
Rust writer, containing UTC-adjusted `true` and local `false` TIME/TIMESTAMP
columns, through `inspect`, `read-cell`, `read-column`, and `read-rows`. It is
development evidence only; it does not grant project, release, evaluation, or
decision authority.

The checked-in `parquet-format-safe` 0.2.4 source splits one generated Thrift
metadata reader into non-inlined helpers. The checked-in `parquet2` 0.17.2
source replaces its 33-way const-generic `u32` bit-unpack dispatcher with an
equivalent bounded dynamic loop and hardens malformed RLE/page boundaries used
by this decoder. Together these patches keep the compiled component within
Sigil's fixed structural-nesting limit, make corrupt input fail closed, and
avoid rewriting the finished Wasm binary.

Official versions are published once from independently reviewed candidate
artifacts by the repository's keyless GitHub OIDC workflow. Public tags and
release assets are immutable; a conflicting release burns that SemVer.
