#!/usr/bin/env node
import { source, emit, Lock } from "../src/index"
import { optparser, arg, param, oneof } from "../src/optparse"

import * as path from "path"

import * as DB from "@duckdb/node-api"

const selection = optparser(
  arg("file").required(),
  param("-c", "--columns").as("columns")
)

const opts_src = optparser(oneof(selection).as("selections").repeat())

let args = opts_src.parse()

source(async () => {
  const files = args.selections
  let prev_collection = ""

  for (let file of files) {
    const db_inst = await DB.DuckDBInstance.create(":memory:", {})
    const db = await db_inst.connect()
    let collection = path.basename(file.file).replace(/(-\d*)?\.[^\.]*$/, "")

    if (prev_collection !== collection) {
      await emit.collection(collection)
      prev_collection = collection
    }

    const stmt = await db.prepare(
      `SELECT ${file.columns ?? "*"} FROM read_parquet($file)`
    )
    stmt.bind({
      file: file.file,
    })

    const reader = await stmt.streamAndRead()

    do {
      await reader.readUntil(1024)
      for (let row of reader.getRowObjectsJson()) {
        emit.data(row)
      }

      if (reader.done) {
        break
      }
    } while (true)

    db_inst.closeSync()
  }
})
