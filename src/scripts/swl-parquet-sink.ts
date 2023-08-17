#!/usr/bin/env -S node --enable-source-maps
import { sink, Sink, } from "../index"
import { duckdb_sink } from "./duckdb-sink-common"

sink(async (): Promise<Sink> => {
  const sink = await duckdb_sink(":memory:", { verbose: 2 })
  const end = sink.end

  sink.end = function () {
    end()
    console.error("???")
  }

  return sink
})
