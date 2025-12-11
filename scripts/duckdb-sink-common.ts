import * as DB from "@duckdb/node-api"
import {
  Collection,
  CollectionHandler,
  Sink,
  col_table,
  log2,
  log3,
} from "../src"
import { Type } from "schema"

// COMMON

//
export interface DuckDBSinkOptions {
  drop?: boolean
  truncate?: boolean
  upsert?: boolean
  verbose: number
}

function _type_to_duckdb_type(type: Type): string {
  if (typeof type === "string") {
    return type
  }
  if (type.type === "LIST") {
    return _type_to_duckdb_type(type.value) + "[]"
  }
  if (type.type === "MAP") {
    return (
      "MAP(" +
      _type_to_duckdb_type(type.key) +
      ", " +
      _type_to_duckdb_type(type.value) +
      ")"
    )
  }
  if (type.type === "STRUCT") {
    return (
      "STRUCT(" +
      type.columns
        .map((c) => c.column_name + " " + _type_to_duckdb_type(c.column_type))
        .join(", ") +
      ")"
    )
  }
  if (type.type === "UNION") {
    return (
      "UNION(" +
      type.members.map((m) => _type_to_duckdb_type(m)).join(", ") +
      ")"
    )
  }
  throw new Error(`Unknown type: ${type.type}`)
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
    const helpers = col.columns

    var types = helpers
      ? helpers.map((c) => _type_to_duckdb_type(c.column_type))
      : columns.map((c) =>
          typeof start[c] === "number"
            ? "float"
            : start[c] instanceof Buffer
            ? "blob"
            : "text"
        )
    const column_names = helpers
      ? helpers.map((c) => c.column_name)
      : columns.map((c) => `"${c}"`)

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

    const desc = await db.runAndReadAll(
      `SELECT json_group_object(column_name, data_type)::varchar as struct_type FROM information_schema.columns WHERE table_schema = '${schema}' AND table_name = '${table}'`
    )
    const struct_type = desc.getRowObjectsJson()[0].struct_type
    console.error(struct_type)
    await exec(`CREATE TEMP TABLE __temp__json (data varchar)`)
    // console.error(schema, table)

    const appender = await db.createAppender("__temp__json")
    let nb_values = 0

    return {
      data(data) {
        nb_values++

        appender.appendVarchar(JSON.stringify(data))
        appender.endRow()

        if (nb_values % 1024 === 0) {
          appender.flushSync()
        }
        // stmt.run(...columns.map((c) => data[c]))
      },
      async end() {
        appender.closeSync()
        await db.run(
          `INSERT INTO "${schema}"."${table}"(${column_names
            .map((c) => (c[0] === '"' ? c : `"${c}"`))
            .join(", ")}) SELECT ${column_names
            .map((c) => `json.${c}`)
            .join(
              ", "
            )} from (select from_json(js.data, $$${struct_type}$$) as json from __temp__json js) json `
        )
        await db.run(`DROP TABLE __temp__json`)
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
      await db.run("CHECKPOINT")
      db_inst.closeSync()

      log2("commit")
    },
  }
}
