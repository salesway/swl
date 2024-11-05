#!/usr/bin/env -S bun run

import { Connection, createConnection } from 'promise-mysql'

import { default_opts, emit, log1, source, uri_maybe_open_tunnel, col_src } from '../index'
import { optparser, arg, param, oneof } from "../optparse"


const opts_src = optparser(
  arg("name").required(),
  param("-q", "--query").as("query"),
)

const opts = optparser(
  default_opts,
  // param("-s", "--schema").as("schema").default("public"),
  arg("uri").required(),
  oneof(opts_src).as("sources").repeat(),
).parse()

/**
 *
 */
source(async function pg_source() {
  let open = await uri_maybe_open_tunnel(opts.uri)
  let uri = open.uri.startsWith("mysql://") ? open.uri : `mysql://${open.uri}`

  let client = await createConnection(uri)
  async function process() {
    // log("connected")

    let queries = opts.sources.length ?
      opts.sources.map(s => ({ name: s.name, query: s.query ?? /* sql */`select * from ${s.name} TBL` }))
      : await get_all_tables(client)

    for (let q of queries) {
      const result = await client.query(q.query)
      emit.collection(q.name)
      for (let r of result) {
        // console.error(r)
        emit.data(r)
      }
    }
    // log("queries; ", queries)
  }

  try {
    log1("connected to", col_src(uri))
    await process()
  } finally {
    client.end()
  }
})


async function get_all_tables(client: Connection) {
  let tbls = await client.query(/* sql */ `
    SELECT table_name as name FROM information_schema.tables WHERE table_type = 'base table'
  `)

  return tbls.map((t: {name: string}) => ({ name: t.name, query: /* sql */ `SELECT * FROM ${t.name} TBL` }))
}