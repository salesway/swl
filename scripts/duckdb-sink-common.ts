import * as DB from "@duckdb/node-api"
import {
  Collection,
  CollectionHandler,
  Lock,
  Sink,
  col_table,
  log2,
  log3,
} from "../src"

// COMMON

//
export interface DuckDBSinkOptions {
  drop?: boolean
  truncate?: boolean
  upsert?: boolean
  verbose: number
}

export async function duckdb_sink(
  path: string,
  opts: DuckDBSinkOptions
): Promise<Sink> {
  const db_inst = await DB.DuckDBInstance.create(path, {})
  const db = await db_inst.connect()

  async function exec(stmt: string) {
    log3(stmt)
    await db.run(stmt)
  }

  async function collection_handler(
    col: Collection,
    start: any
  ): Promise<CollectionHandler> {
    const name = col.name
    let table = col.name
    var columns = Object.keys(start)

    var types = columns.map((c) =>
      typeof start[c] === "number"
        ? "float"
        : start[c] instanceof Buffer
        ? "blob"
        : "text"
    )

    let schema = "main"
    if (name.includes(".")) {
      const [_schema, tbl] = name.split(".")
      // console.error(_schema, tbl)
      schema = _schema
      table = tbl
      await exec(`CREATE SCHEMA IF NOT EXISTS "${schema}"`)
    }

    if (opts.drop) {
      log2("dropping", col_table(table))
      await exec(`DROP TABLE IF EXISTS "${schema}".${table}`)
    }

    await exec(`CREATE TABLE IF NOT EXISTS ${schema}.${table} (
      ${columns.map((c, i) => `"${c}" ${types[i]}`).join(", ")}
    )`)

    if (opts.truncate) {
      log2("truncating", col_table(table))
      await exec(`DELETE FROM "${schema}".${table}`)
    }

    await exec(
      `CREATE TEMP TABLE temp.${table} (${columns
        .map((c, i) => `"${c}" ${types[i]}`)
        .join(", ")})`
    )
    // console.error(schema, table)

    const appender = await db.createAppender(table, schema)
    let nb_values = 0

    return {
      data(data) {
        nb_values++

        for (let i = 0; i < columns.length; i++) {
          let col = columns[i]
          let original_value = data[col]
          let type: DB.DuckDBType
          let value: any
          if (original_value == null) {
            appender.appendNull()
            continue
          }

          switch (types[i]) {
            case "float":
              type = DB.FLOAT
              value = Number(data[col])
              break

            case "text":
            default:
              type = DB.VARCHAR
              value = "" + original_value
              break
          }
          appender.appendValue(value, type)
        }

        appender.endRow()

        if (nb_values % 1024 === 0) {
          appender.flushSync()
        }
        // stmt.run(...columns.map((c) => data[c]))
      },
      async end() {
        appender.closeSync()
        appender.appendMap
        await db.run(
          `INSERT ${
            opts.upsert ? "OR REPLACE" : ""
          } INTO "${schema}"."${table}"(${columns
            .map((c) => `"${c}"`)
            .join(", ")}) SELECT * FROM temp.${table}`
        )
        await db.run(`DROP TABLE temp.${table}`)
        if (opts.verbose >= 2) {
          // let s = db.prepare(`select count(*) as cnt from "${tale}`)
          // log2("table", col_table(table), "now has", col_num(s.all()[0].cnt), "rows")
        }
      },
    }
  }

  await db.run("BEGIN")
  return {
    collection(col, start) {
      return collection_handler(col, start)
    },
    async error(err) {
      log2("rollbacked")
      await db.run("ROLLBACK")
    },
    async end() {
      await db.run("COMMIT")
      db_inst.closeSync()

      log2("commit")
    },
  }
}
