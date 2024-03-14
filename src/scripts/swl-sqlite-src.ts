#!/usr/bin/env -S node --enable-source-maps

import { arg, oneof, optparser } from "../optparse"
import { log2, emit, source, default_opts, file, default_col_sql_src_opts } from "../index"
import * as DB from "better-sqlite3"

let src_parser = optparser(
  default_col_sql_src_opts,
)

let opt_parser = optparser(
  arg("file").required().help("the database file to open"),
  default_opts,
  oneof(src_parser).as("collections").repeat().help("if provided, the collections to extract"),
).prelude("Output collections to an SWL pipeline from an sqlite database")

let opts = opt_parser.parse()


source(() => {
  let db = new DB(opts.file, {readonly: true, fileMustExist: true, })
  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_stat%'`)
      .pluck()

    sources = st.all().map((name: string) => ({name, query: undefined, rename: ""}))
  }

  for (var source of sources) {
    var sql = source.query ?? `SELECT * FROM "${source.name}"`

    var stmt = db.prepare(sql)

    emit.collection(source.name)
    var iterator = (stmt as any).iterate() as IterableIterator<any>
    const jsonb_cols = stmt.columns().filter(c => c.type?.toUpperCase().startsWith("JSON") && c.column)
    for (var s of iterator) {
      if (jsonb_cols.length) {
        for (let col of jsonb_cols) {
          try {
            s[col.column!] = JSON.parse(s[col.column!])
          } catch (e) { }
        }
      }
      emit.data(s)
    }
  }

  log2("finished sending")
})

