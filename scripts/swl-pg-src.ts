#!/usr/bin/env -S bun run

import { Client as PgClient, QueryResultBase } from "pg"
import Cursor from "pg-cursor"

import {
  col_src,
  default_opts,
  emit,
  log1,
  source,
  uri_maybe_open_tunnel,
} from "../src/"
import { arg, oneof, optparser, param } from "../src/optparse"
import { Column, Type } from "schema"

const opts_src = optparser(
  arg("name").required(),
  param("-q", "--query").as("query")
)

const opts = optparser(
  default_opts,
  param("-s", "--schema").as("schema").default("public"),
  arg("uri")
    .help(
      "a postgres connection uri such as postgres://user:pass@host/database"
    )
    .required(),
  oneof(opts_src).as("sources").repeat()
).parse()

let types!: Map<number, PgType>
let relations!: Map<number, PgRelation>
/**
 *
 */
source(async function pg_source() {
  let open = await uri_maybe_open_tunnel(opts.uri, 5432)
  let uri = open.uri.startsWith("postgres://")
    ? open.uri
    : `postgres://${open.uri}`

  const client = new PgClient(uri)

  async function _process() {
    types = await get_types(client)
    relations = await get_relations(client)

    let queries = opts.sources.length
      ? (
          await Promise.all(
            opts.sources.map(async (s) => {
              if (s.name.endsWith(".*")) {
                return await get_all_tables_from_schema(
                  client,
                  s.name.slice(0, -2)
                )
              }
              return [
                {
                  name: s.name,
                  query: s.query || /* sql */ `select * from ${s.name} TBL`,
                },
              ]
            })
          )
        ).flat()
      : await get_all_tables_from_schema(client, opts.schema)

    for (let q of queries) {
      const cursor = new Cursor(q.query)
      const result = (await client.query(cursor)) as Cursor & {
        _result: QueryResultBase
      }

      let emitted = false
      let rows: any[] = []
      do {
        rows = await cursor.read(10000)
        if (!emitted) {
          const helpers: Column[] = result._result.fields.map((f) => {
            const t = types!.get(f.dataTypeID)!
            // FIXME : should get table ID from _result.fields and get the relation to make sure there is not a not null

            return {
              column_name: f.name,
              column_type: pg_type_to_type(t),
              not_null: t.typnotnull,
            }
          })
          // console.error(helpers)
          emit.collection(q.name, helpers.length ? helpers : undefined)
          emitted = true
        }
        for (let r of rows) {
          emit.data(r)
        }
      } while (rows.length > 0)
    }
    // log("queries; ", queries)
  }

  try {
    await client.connect()
    log1("connected to", col_src(uri))
    await _process()
  } finally {
    await client.end()
  }
})

function _array(t: PgType, dim: number): Type {
  return {
    type: "LIST",
    value:
      dim > 1
        ? _array(types!.get(t.typelem)!, dim - 1)
        : pg_type_to_type(types!.get(t.typelem)!),
  }
}

function pg_type_to_type(t: PgType): Type {
  const pg_type = t.typname.toLocaleLowerCase()

  // fixme: needs records

  // this is an array type
  if (t.typelem > 0 && t.typinput === "array_in") {
    return _array(t, t.typndims)
  }

  if (t.typbasetype > 0) {
    // domain
    return pg_type_to_type(types!.get(t.typbasetype)!)
  }

  if (pg_type === "uuid") return "UUID"
  if (pg_type === "text") return "VARCHAR"
  if (pg_type === "date") return "DATE"
  if (pg_type === "json" || pg_type === "jsonb") return "JSON"
  if (pg_type === "char") return "TINYINT"
  if (pg_type === "int2") return "SMALLINT"
  if (pg_type === "int4") return "INTEGER"
  if (pg_type === "int8") return "BIGINT"
  if (pg_type === "float4") return "FLOAT"
  if (pg_type === "float8") return "DOUBLE"
  if (pg_type === "bool") return "BOOLEAN"
  if (pg_type === "varchar") return "VARCHAR"
  if (pg_type === "uuid") return "UUID"
  if (pg_type === "time") return "TIME"
  if (pg_type === "hstore") {
    return { type: "MAP", key: "VARCHAR", value: "VARCHAR" }
  }

  throw new Error(`Unknown type: ${pg_type}`)
}

export interface PgColumn {
  name: string
  typeid: number
  not_null: boolean
}

export interface PgRelation {
  oid: number
  name: string
  schema: string
  columns: PgColumn[]
}

export interface PgType {
  oid: number
  typname: string
  typnamespace: number
  typowner: number
  typlen: number
  typbyval: boolean
  typtype: string
  typcategory: string
  typispreferred: boolean
  typisdefined: boolean
  typdelim: string
  typrelid: number
  typsubscript: string
  typelem: number
  typarray: number
  typinput: string
  typoutput: string
  typreceive: string
  typsend: string
  typmodin: string
  typmodout: string
  typanalyze: string
  typalign: string
  typstorage: string
  typnotnull: boolean
  typbasetype: number
  typtypmod: number
  typndims: number
  typcollation: number
  typdefaultbin: string | null
  typdefault: string | null
  typacl: string | null
}

async function get_relations(
  client: PgClient
): Promise<Map<number, PgRelation>> {
  const relations_q = await client.query(/* sql */ `select * from pg_class`)
  const rows: Map<number, PgRelation> = relations_q.rows.reduce((acc, item) => {
    acc.set(item.oid, item)
    return acc
  }, new Map())
  return rows
}

async function get_types(client: PgClient): Promise<Map<number, PgType>> {
  const types_q = await client.query(/* sql */ `select * from pg_type`)
  const rows: Map<number, PgType> = types_q.rows.reduce((acc, item) => {
    acc.set(item.oid, item)
    return acc
  }, new Map())
  return rows
}

async function get_all_tables_from_schema(client: PgClient, schema: string) {
  let tbls = await client.query(/* sql */ `
    WITH cons AS (SELECT
      tc.table_schema,
      tc.constraint_name,
      tc.table_name,
      kcu.column_name,
      ccu.table_schema AS foreign_table_schema,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'),
        tbls AS (SELECT tbl.table_schema as "schema", tbl.table_schema || '.' || tbl.table_name as tbl, cons.foreign_table_schema || '.' || cons.foreign_table_name as dep FROM
        "information_schema"."tables" tbl
        LEFT JOIN cons ON cons.table_name = tbl.table_name AND cons.table_schema = tbl.table_schema
    where tbl.table_schema = '${schema}' AND tbl.table_type = 'BASE TABLE')
    SELECT t.tbl, COALESCE(array_agg(t.dep) FILTER (WHERE t.dep IS NOT NULL), '{}'::text[]) as deps
    FROM tbls t
    GROUP BY t.tbl
  `)

  const dct = {} as { [name: string]: string[] }
  for (let r of tbls.rows) {
    dct[r.tbl] = r.deps
  }

  const tables_set = new Set<string>()
  const add_deps = (tbl: string) => {
    for (var t of dct[tbl]) {
      if (!tables_set.has(t) && t !== tbl) add_deps(t)
    }
    tables_set.add(tbl)
  }

  for (var tblname in dct) {
    add_deps(tblname)
  }
  let keys = Array.from(tables_set)
  return keys.map((k) => ({
    name: k,
    query: /* sql */ `SELECT * FROM ${k} TBL`,
  }))
}
