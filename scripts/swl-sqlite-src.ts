#!/usr/bin/env -S bun run

import { arg, oneof, optparser } from "../src/optparse"
import { log2, emit, source, default_opts, file, default_col_sql_src_opts } from "../src/index"
import { Database } from "bun:sqlite"

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
  let db = new Database(opts.file, {readonly: true, create: false })
  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare<{name: string}, []>(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_stat%'`)


    sources = st.all().map(r => ({name: r.name, query: "", rename: ""}))
  }

  for (var source of sources) {
    var sql = source.query || `SELECT * FROM "${source.name}"`

    var stmt = db.prepare(sql)

    emit.collection(source.name)
    var iterator = (stmt as any).iterate() as IterableIterator<any>

    for (var s of iterator) {
      emit.data(s)
    }
  }

  log2("finished sending")
})

