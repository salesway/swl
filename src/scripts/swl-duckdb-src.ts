#!/usr/bin/env -S node --enable-source-maps

import { arg, oneof, optparser } from "../optparse"
import { log2, emit, source, default_opts, file, default_col_sql_src_opts, Lock, log3 } from "../index"
import * as DB from "duckdb"

let src_parser = optparser(
  default_col_sql_src_opts,
)

let opt_parser = optparser(
  arg("file").required().help("the database file to open"),
  default_opts,
  oneof(src_parser).as("collections").repeat().help("if provided, the collections to extract"),
).prelude("Output collections to an SWL pipeline from an sqlite database")

let opts = opt_parser.parse()

source(async () => {
  let db = new DB.Database(opts.file, DB.OPEN_READONLY)
  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const lock = new Lock<{table_name: string, table_schema: string}[]>()
    db.all(`SELECT * FROM information_schema.tables`, lock.callback)
    const res = await lock.promise

    sources = res.map(r => ({name: r.table_schema !== "main" ? `${r.table_schema}.${r.table_name}` : r.table_name, query: undefined, rename: "", schema: r.table_schema}))
  }

  for (var source of sources) {
    var sql = source.query ?? `SELECT * FROM ${source.name}`

    log3(sql)
    var stmt = db.prepare(sql)

    const lock = new Lock<void>()

    emit.collection(source.name)
    stmt.each((_, row) => { emit.data(row) })

    stmt.finalize((err) => {
      if (err != null) {
        lock.reject(err)
      } else {
        lock.resolve()
      }
    })

    await lock.promise
  }

  log2("finished sending")
  db.close()
})

