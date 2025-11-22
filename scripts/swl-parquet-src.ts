#!/usr/bin/env node
import { source, emit } from "../src/index"
import { DescribeResult } from "../src/schema"
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

    const sql = `SELECT ${file.columns ?? "*"} FROM read_parquet('${
      file.file
    }')`
    const desc = await db.runAndReadAll("DESCRIBE " + sql)
    console.error(desc.getRowObjectsJson() as DescribeResult[])

    if (prev_collection !== collection) {
      await emit.collection(collection)
      prev_collection = collection
    }

    const stmt = await db.prepare(sql)

    const reader = await stmt.streamAndRead()

    let last = 0
    do {
      await reader.readUntil(last + 2048)
      last = reader.currentRowCount

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
