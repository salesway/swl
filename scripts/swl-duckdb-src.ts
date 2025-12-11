#!/usr/bin/env -S bun run

import { arg, oneof, optparser } from "../src/optparse"
import {
  log2,
  emit,
  source,
  default_opts,
  file,
  default_col_sql_src_opts,
  log3,
} from "../src/index"
import * as DB from "@duckdb/node-api"
import { Column, DescribeResult, parse_duckdb_describe_type } from "schema"

let src_parser = optparser(default_col_sql_src_opts)

let opt_parser = optparser(
  arg("file").required().help("the database file to open"),
  default_opts,
  oneof(src_parser)
    .as("collections")
    .repeat()
    .help("if provided, the collections to extract")
).prelude("Output collections to an SWL pipeline from an sqlite database")

let opts = opt_parser.parse()

source(async () => {
  let db_inst = await DB.DuckDBInstance.create(opts.file, {}) //new DB.Database(opts.file, DB.OPEN_READONLY)
  let db = await db_inst.connect()

  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const res = await db
      .runAndReadAll(`SELECT * FROM information_schema.tables`)
      .then(
        (r) =>
          r.getRowObjectsJson() as {
            table_name: string
            table_schema: string
          }[]
      )

    sources = res.map((r) => ({
      name:
        r.table_schema !== "main"
          ? `${r.table_schema}.${r.table_name}`
          : r.table_name,
      query: undefined,
      rename: "",
      schema: r.table_schema,
    }))
  }

  for (var source of sources) {
    var sql = source.query ?? `SELECT * FROM ${source.name}`

    log3(sql)
    const description = (
      await db.runAndReadAll(`DESCRIBE ${sql}`)
    ).getRowObjectsJson() as DescribeResult[]

    const columns: Column[] = description.map((desc) => ({
      column_name: desc.column_name,
      column_type: parse_duckdb_describe_type(desc.column_type),
      not_null: desc.null,
    }))

    var stmt = await db.prepare(`SELECT to_json(J) as json FROM (${sql}) J`)
    const stream = await stmt.stream()

    // const reader = await stmt.streamAndRead()
    emit.collection(source.name, columns)

    do {
      const chunk = await stream.fetchChunk()
      if (chunk == null || chunk.rowCount === 0) {
        break
      }
      for (const row of chunk.getRowObjects(["json"])) {
        emit.data(JSON.parse(row.json as string))
      }
    } while (true)
  }

  //   let last = 0
  //   let cnt = 0
  //   do {
  //     let prev = last
  //     await reader.readUntil(last + 2048)
  //     await reader.getRowObjectsJson

  //     last = reader.currentRowCount
  //     console.error(last, prev)

  //     console.error(reader.getRowObjectsJson().length, "rows")
  //     for (const row of reader.getRowObjectsJson()) {
  //       cnt++
  //       emit.data(row)
  //     }
  //     console.error(cnt, "emitted")

  //     if (reader.done) {
  //       break
  //     }
  //   } while (true)
  // }

  log2("finished sending")
  db_inst.closeSync()
})
