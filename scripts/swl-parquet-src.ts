#!/usr/bin/env node
import { source, emit, Lock } from "../src/index"
import { optparser, arg, param, oneof } from "../src/optparse"

import * as path from "path"

import * as DB from "duckdb"

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
    const db = new DB.Database(":memory:")
    let collection = path.basename(file.file).replace(/(-\d*)?\.[^\.]*$/, "")

    if (prev_collection !== collection) {
      await emit.collection(collection)
      prev_collection = collection
    }

    const stmt = db.prepare(
      `SELECT ${file.columns ?? "*"} FROM read_parquet($1)`
    )
    const lock = new Lock<void>()

    stmt.each(file.file, (_, row) => {
      emit.data({ ...row })
    })

    stmt.finalize(() => {
      lock.resolve()
    })

    await lock.promise
    db.close()
  }
})
