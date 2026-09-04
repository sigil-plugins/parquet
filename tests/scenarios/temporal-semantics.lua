local function from_hex(encoded)
  return encoded:gsub("..", function(pair)
    return string.char(tonumber(pair, 16))
  end)
end

return {
  title = "Preserve Parquet UTC-adjustment semantics",
  priority = "P0",
  policy = { capabilities = { "wasm.parquet" } },

  run = function()
    local parquet = require("wasm.parquet")
    local bytes = from_hex(sigil.env("PARQUET_FIXTURE_HEX"))

    local info, inspect_err = parquet.inspect(bytes)
    expect(info ~= nil, inspect_err and inspect_err.message)
    expect(info.columns[1].path == "utc_timestamp")
    expect(info.columns[1]["is-adjusted-to-utc"] == true)
    expect(info.columns[2].path == "local_timestamp")
    expect(info.columns[2]["is-adjusted-to-utc"] == false)
    expect(info.columns[3].path == "utc_time")
    expect(info.columns[3]["is-adjusted-to-utc"] == true)
    expect(info.columns[4].path == "local_time")
    expect(info.columns[4]["is-adjusted-to-utc"] == false)

    local local_timestamp, cell_err = parquet["read-cell"](bytes, {
      column = "local_timestamp",
      row = 0,
    })
    expect(local_timestamp ~= nil, cell_err and cell_err.message)
    expect(local_timestamp.tag == "timestamp")
    expect(local_timestamp.value.unit == "milliseconds")
    expect(local_timestamp.value["is-adjusted-to-utc"] == false)

    local utc_column, column_err = parquet["read-column"](bytes, {
      column = "utc_timestamp",
      offset = 0,
      limit = 2,
    })
    expect(utc_column ~= nil, column_err and column_err.message)
    expect(utc_column[1].value["is-adjusted-to-utc"] == true)
    expect(utc_column[2].value["is-adjusted-to-utc"] == true)

    local batch, rows_err = parquet["read-rows"](bytes, {
      columns = { "utc_time", "local_time", "local_timestamp" },
      offset = 0,
      limit = 2,
    })
    expect(batch ~= nil, rows_err and rows_err.message)
    expect(batch.rows[1].cells[1].value["is-adjusted-to-utc"] == true)
    expect(batch.rows[1].cells[2].value["is-adjusted-to-utc"] == false)
    expect(batch.rows[2].cells[3].value["is-adjusted-to-utc"] == false)
  end,
}
