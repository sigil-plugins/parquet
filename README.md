# Sigil Parquet plugin

`wasm.parquet` is a bounded, read-only Parquet inspector and typed scalar
reader for Sigil scenarios. Version 0.1 accepts complete Parquet file bytes,
reports flat leaf-column metadata, and reads one cell selected by column path
and zero-based row index.

It composes directly with byte-producing plugins such as `wasm.s3`:

```lua
local s3 = require("wasm.s3")
local parquet = require("wasm.parquet")

local bytes, get_err = s3["get-object"]({
  endpoint = "minio",
  bucket = "results",
  key = "run/output.parquet",
  ["max-bytes"] = 4 * 1024 * 1024,
})
expect(bytes ~= nil, get_err and get_err.message)

local cell, read_err = parquet["read-cell"](bytes, {
  column = "total",
  row = 0,
})
expect(cell ~= nil, read_err and read_err.message)
expect(cell.tag == "floating")
expect(cell.value == 27.75)
```

Add both project locks before evaluating the scenario:

```bash
sigil plugin add s3@0.1.0
sigil plugin add parquet@0.1.0
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
values, and 32 MiB uncompressed column chunks. Parser and codec errors are
collapsed to stable messages rather than exposing input-derived details.

Build, test both compression fixtures, validate the component, and pack a
local candidate:

```bash
just check
just dist
```

Install that unpublished package into the per-user store before adding its
project lock:

```bash
sigil plugin install --path dist/parquet-0.1.0.sigil-plugin.tar.zst
sigil plugin add parquet@0.1.0
```

The checked-in `parquet-format-safe` 0.2.4 source splits one generated Thrift
metadata reader into non-inlined helpers. The checked-in `parquet2` 0.17.2
source replaces its 33-way const-generic `u32` bit-unpack dispatcher with an
equivalent bounded dynamic loop and hardens malformed RLE/page boundaries used
by this decoder. Together these patches keep the compiled component within
Sigil's fixed structural-nesting limit, make corrupt input fail closed, and
avoid rewriting the finished Wasm binary.

This repository is a local candidate until its source, exact component/package
bytes, release policy, and official namespace publication receive the separate
human gate required by Sigil's immutable plugin process.
