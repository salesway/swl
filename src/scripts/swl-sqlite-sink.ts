#!/usr/bin/env -S node --enable-source-maps

import { log2, log3, sink, CollectionHandler, Sink, default_opts, col_table, col_num } from "../index"
import { optparser, arg, oneof, flag } from "../optparse"

import * as DB from "better-sqlite3"
import { file } from "../debug"

let col_opts = optparser(
  flag("-t", "--truncate").as("truncate"),
  flag("-d", "--drop").as("drop"),
  flag("-u", "--upsert").as("upsert")
)

let col_parser = optparser(
  arg("name").required(),
  col_opts,
)

let opts_parser = optparser(
  arg("file").required(),
  default_opts,
  col_opts,
  oneof(col_parser).as("collections").repeat(),
)

let opts = opts_parser.parse()

for (let c of opts.collections) {
  if (opts.truncate) c.truncate = true
  if (opts.drop) c.drop = true
  if (opts.upsert) c.upsert = true
}


function exec(stmt: string) {
  log3(stmt)
  db.exec(stmt)
}

function collection_handler(name: string, start: any): CollectionHandler {
  let table = name
  var columns = Object.keys(start)

  var types = columns.map(c => typeof start[c] === "number" ? "int"
  : start[c] instanceof Buffer ? "BLOB"
  : start[c]?.constructor === Object || Array.isArray(start[c]) ? "JSONB"
  : "TEXT")

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
    // console.error(sql)
    stmt = db.prepare(sql)
  } else if (opts.upsert) {
    // Should I do some sub-query thing with coalesce ?
    // I would need some kind of primary key...
    // console.error(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(", ")})
    // values (${columns.map(c => "?").join(", ")})`)
    stmt = db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(", ")})
      values (${columns.map(c => "?").join(", ")})`)
  }

  return {
    data(data) {
      stmt.run(...columns.map(c => {
        let v = data[c]
        if (v instanceof Date) return v.toJSON()
        if (v && typeof v === 'object' && !(v instanceof Buffer)) return JSON.stringify(v)
        if (typeof v === "boolean") return v ? 1 : 0
        // console.debug(typeof v, v?.constructor?.name)
        return v
      }))
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
// db.pragma("journal_mode = wal")
db.pragma("journal_mode = wal")
db.pragma("synchronous = 0")
// db.pragma("locking_mode = EXCLUSIVE")

sink((): Sink => {
  db.exec("BEGIN")
  return {
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
      db.pragma("journal_mode = delete")
      db.close()
    },
  }

})
