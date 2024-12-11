
import * as DB from "duckdb"
import { CollectionHandler, Lock, Sink, col_table, log2, log3 } from ".."

// COMMON

//
export interface DuckDBSinkOptions {
  drop?: boolean,
  truncate?: boolean,
  upsert?: boolean,
  verbose: number
}

export function duckdb_sink(path: string, opts: DuckDBSinkOptions): Sink {

  const db = new DB.Database(path)

  async function exec(stmt: string) {
    log3(stmt)
    const lock = new Lock()
    db.exec(stmt, function (err, res) {
      if (err != null) {
        lock.reject(err)
      } else {
        lock.resolve(res)
      }
    })
    await lock.promise
  }

  async function collection_handler(name: string, start: any): Promise<CollectionHandler> {
    let table = name
    var columns = Object.keys(start)

    var types = columns.map(c => typeof start[c] === "number" ? "int"
    : start[c] instanceof Buffer ? "blob"
    : "text")

    if (name.includes(".")) {
      const [schema, tbl] = name.split(".")
      console.error(schema, tbl)
      await exec(`CREATE SCHEMA IF NOT EXISTS "${schema}"`)
      table = `"${schema}"."${tbl}"`
    } else {
      table =`"${table}"`
    }

    if (opts.drop) {
      log2("dropping", col_table(table))
      await exec(`DROP TABLE IF EXISTS ${table}`)
    }

    // Create if not exists ?
    // Temporary ?
    await exec(`CREATE TABLE IF NOT EXISTS ${table} (
      ${columns.map((c, i) => `"${c}" ${types[i]}`).join(", ")}
    )`)

    if (opts.truncate) {
      log2("truncating", col_table(table))
      await exec(`DELETE FROM ${table}`)
    }

    let stmt!: DB.Statement
    if (!opts.upsert) {
      const sql = `INSERT INTO ${table} (${columns.map(c => `"${c}"`).join(", ")})
      values (${columns.map(c => "?").join(", ")})`
      // console.error(sql)
      stmt = db.prepare(sql)
    } else if (opts.upsert) {
      // Should I do some sub-query thing with coalesce ?
      // I would need some kind of primary key...
      // console.error(`INSERT OR REPLACE INTO ${table} (${columns.map(c => `"${c}"`).join(", ")})
      // values (${columns.map(c => "?").join(", ")})`)
      stmt = db.prepare(`INSERT OR REPLACE INTO ${table} (${columns.map(c => `"${c}"`).join(", ")})
        values (${columns.map(c => "?").join(", ")})`)
    }

    return {
      data(data) {
        stmt.run(...columns.map(c => data[c]))
      },
      end() {
        if (opts.verbose >= 2) {
          // let s = db.prepare(`select count(*) as cnt from "${tale}`)
          // log2("table", col_table(table), "now has", col_num(s.all()[0].cnt), "rows")
        }
      }
    }
  }

  db.exec("BEGIN")
  return {
    collection(col, start) {
      return collection_handler(col.name, start)
    },
    error(err) {
      log2("rollbacked")
      db.exec("ROLLBACK")
    },
    end() {
      db.exec("COMMIT")
      log2("commit")
    }
  }

}
