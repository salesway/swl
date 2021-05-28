#!/usr/bin/env -S node --enable-source-maps

import { log2, log3, sink, optparser, CollectionHandler, Handler, default_opts, col_table, col_num } from "../index"
import * as DB from "better-sqlite3"
import { file } from "../debug"

let col_parser = optparser()
  .arg("name")
  .flag("truncate", {short: "t", long: "truncate"})
  .flag("drop", {short: "d", long: "drop"})
  .flag("upsert", {short: "u", long: "upsert"})

let opts_parser = optparser()
  .arg("file")
  .include(default_opts)
  .flag("truncate", {short: "t", long: "truncate"})
  .flag("drop", {short: "d", long: "drop"})
  .flag("upsert", {short: "u", long: "upsert"})
  .flag("passthrough", {short: "p", long: "passthrough"})
  .sub("collections", col_parser)
  .post(opts => {
    for (let c of opts.collections) {
      if (opts.truncate) c.truncate = 1
      if (opts.drop) c.drop = 1
      if (opts.upsert) c.upsert = 1
    }

    if (!opts.file) throw new Error("sqlite source expects a file name")
  })

let opts = opts_parser.parse()


function exec(stmt: string) {
  log3(stmt)
  db.exec(stmt)
}

function collection_handler(name: string, start: any): CollectionHandler {
  let table = name
  var columns = Object.keys(start)

  var types = columns.map(c => typeof start[c] === "number" ? "int"
  : start[c] instanceof Buffer ? "blob"
  : "text")

  if (opts.drop) {
    log2("dropping", col_table(table))
    exec(`DROP TABLE IF EXISTS "${table}"`)
  }

  // Create if not exists ?
  // Temporary ?
  exec(`CREATE TABLE IF NOT EXISTS "${table}" (
    ${columns.map((c, i) => `"${c}" ${types[i]}`).join(", ")}
  )`)

  if (opts.truncate) {
    log2("truncating", col_table(table))
    exec(`DELETE FROM "${table}"`)
  }

  let stmt!: DB.Statement
  if (!opts.upsert) {
    const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(", ")})
    values (${columns.map(c => "?").join(", ")})`
    stmt = db.prepare(sql)
  } else if (opts.upsert) {
    // Should I do some sub-query thing with coalesce ?
    // I would need some kind of primary key...
    stmt = db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(", ")})
      values (${columns.map(c => "?").join(", ")})`)
  }

  return {
    data(data) {
      stmt.run(...columns.map(c => data[c]))
    },
    end() {
      if (opts.verbose >= 2) {
        let s = db.prepare(`select count(*) as cnt from "${table}"`)
        log2("table", col_table(table), "now has", col_num(s.all()[0].cnt), "rows")
      }
    }
  }
}

let db = new DB(opts.file, { fileMustExist: false })
log2("opened file", file(opts.file), "to write")
// if (opts.pragma) {

// }

// let journal_mode = db.pragma("journal_mode")
// let synchronous = db.pragma("synchronous")
// let locking_mode = db.pragma("locking_mode")
// db.pragma("journal_mode = off")
// db.pragma("synchronous = 0")
// db.pragma("locking_mode = EXCLUSIVE")

sink((): Handler => {
  db.exec("BEGIN")
  return {
    passthrough: !!opts.passthrough,
    collection(col, start) {
      return collection_handler(col.name, start)
    },
    error() {
      log2("rollbacked")
      db.exec("ROLLBACK")
    },
    end() {
      db.exec("COMMIT")
      log2("commited changes")
      db.close()
    },
  }

})
