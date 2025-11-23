#!/usr/bin/env -S bun run

import { arg, oneof, optparser } from "../src/optparse"
import {
  log2,
  emit,
  source,
  default_opts,
  file,
  default_col_sql_src_opts,
} from "../src/index"
import { Database } from "bun:sqlite"

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

source(() => {
  let db = new Database(opts.file, { readonly: true, create: false })
  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare<{ name: string }, []>(
      `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_stat%'`
    )

    sources = st.all().map((r) => ({ name: r.name, query: "", rename: "" }))
  }

  for (var source of sources) {
    var sql = source.query || `SELECT * FROM "${source.name}"`

    var stmt = db.prepare(sql)

    emit.collection(source.name)

    var iterator = (stmt as any).iterate() as IterableIterator<any>

    let json_columns: undefined | { name: string; type: string }[] = undefined

    for (var s of iterator) {
      if (json_columns == null) {
        json_columns = stmt.declaredTypes
          .map((type, idx) => ({ name: stmt.columnNames[idx], type: type! }))
          .filter((col, idx) => {
            if (!col.type) return false
            const type = col.type.toLowerCase()
            return (
              type.startsWith("struct") ||
              type.startsWith("union") ||
              type.includes("[") ||
              type.includes("(") ||
              type.includes("json")
            )
          })
      }

      for (let j of json_columns) {
        const value = s[j.name]
        if (
          typeof value === "string" &&
          (value[0] === "{" || value[0] === "[")
        ) {
          s[j.name] = JSON.parse(s[j.name])
        }
      }
      emit.data(s)
    }
  }

  log2("finished sending")
})
