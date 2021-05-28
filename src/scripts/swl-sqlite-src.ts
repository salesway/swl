#!/usr/bin/env -S node --enable-source-maps

import { log2, emit, source, optparser, default_opts, file, } from "../index"
import * as DB from "better-sqlite3"

let src_parser = optparser()
  .arg("name")
  .option("query", {short: "q", long: "query"})

let opt_parser = optparser()
  .arg("file")
  .include(default_opts)
  .sub("collections", src_parser)
  .post(opts => {
    if (!opts.file) throw new Error("sqlite source expects a file name")
  })

let opts = opt_parser.parse()

source(() => {
  let db = new DB(opts.file, {readonly: true, fileMustExist: true})
  log2("opened file", file(opts.file), "to read")
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
      .pluck()

    sources = st.all().map((name: string) => {
      let res = src_parser.prebuild()
      res.name = name
      return res
    })
  }

  for (var source of sources) {
    var sql = source.query ?? `SELECT * FROM "${source.name}"`

    var stmt = db.prepare(sql)

    emit.collection(source.name)
    // this.info(`Started ${colname}`)
    var iterator = (stmt as any).iterate() as IterableIterator<any>
    let count = 0
    for (var s of iterator) {
      count++
      emit.data(s)
    }
  }

  log2("finished sending")
})

