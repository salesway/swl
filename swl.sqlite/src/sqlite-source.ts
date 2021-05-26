#!/usr/bin/env -S node --enable-source-maps

import { emit, util, optparser } from "swl"
import * as DB from "better-sqlite3"

import { uncoerce } from "./common"

let src_parser = optparser()
  .arg("name")
  .flag("uncoerce", {short: "u", long: "uncoerce"})
  .option("query", {short: "q", long: "query"})

let opt_parser = optparser()
  .arg("file")
  .flag("uncoerce", {short: "u", long: "uncoerce"})
  .sub("collections", src_parser)
  .post(opts => {
    if (!opts.file) throw new Error("sqlite source expects a file name")
    if (opts.uncoerce) {
      for (let c of opts.collections) c.uncoerce = true
    }
  })

let opts = opt_parser.parse()

util.source(() => {
  let db = new DB(opts.file, {readonly: true, fileMustExist: true})
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
      .pluck()

    sources = st.all().map((name: string) => {
      let res = src_parser.prebuild()
      if (opts.uncoerce) res.uncoerce = true
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
    for (var s of iterator) {
      if (source.uncoerce) {
        var s2: any = {}
        for (var x in s)
          s2[x] = uncoerce(s[x])
        s = s2
      }

      emit.data(s)
    }
  }

})

