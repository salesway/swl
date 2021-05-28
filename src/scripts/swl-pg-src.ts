#!/usr/bin/env -S node --enable-source-maps

import { Client as PgClient } from 'pg'

import { default_opts, emit, optparser, source, uri_maybe_open_tunnel } from '../index'


const opts = optparser()
  .include(default_opts)
  .arg("uri")
  .option("schema", { short: "s", help: "schema from which get", default: "public" })
  .sub("sources", optparser()
    .arg("name")
    .option("query", { short: "q", help: "query" })
  )
  .post(opts => {
    if (!opts.uri) throw new Error(`pg source expects an uri`)
  })
  .parse()

source(async function pg_source() {
  let open = await uri_maybe_open_tunnel(opts.uri)
  let uri = open.uri.startsWith("postgres://") ? open.uri : `postgres://${open.uri}`

  let client = new PgClient(uri)
  async function process() {
    // log("connected")

    let queries = opts.sources.length ?
      opts.sources.map(s => ({ name: s.name, query: s.query ?? /* sql */`select * from ${s.name}` }))
      : await get_all_tables_from_schema(client, opts.schema)

    for (let q of queries) {
      const result = await client.query(q.query)
      emit.collection(q.name)
      for (let r of result.rows) {
        emit.data(r)
      }
    }
    // log("queries; ", queries)
  }

  try {
    await client.connect()
    await process()
  } finally {
    client.end()
    open.tunnel?.close()
  }
})


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

  const dct = {} as {[name: string]: string[]}
  for (let r of tbls.rows) {
    dct[r.tbl] = r.deps
  }

  const tables_set = new Set<string>()
  const add_deps = (tbl: string) => {
    for (var t of dct[tbl]) {
      if (!tables_set.has(t) && t !== tbl)
        add_deps(t)
    }
    tables_set.add(tbl)
  }

  for (var tblname in dct) {
    add_deps(tblname)
  }
  let keys = Array.from(tables_set)
  return keys.map(k => ({ name: k, query: /* sql */ `SELECT * FROM ${k}` }))
}