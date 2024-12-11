#!/usr/bin/env -S bun run
import { sink, Sink, } from "../src/index"
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
