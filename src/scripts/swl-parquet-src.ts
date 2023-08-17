#!/usr/bin/env node
import { source, emit, Lock, } from "../index"
import { optparser, arg, } from "../optparse"

import * as path from "path"

import * as DB from "duckdb"

const opts_src = optparser(
  arg("files").required().repeat(),
)

let args = opts_src.parse()

source(async () => {

  const files = args.files
  let prev_collection = ""

  for (let file of files) {
    const db = new DB.Database(":memory:")
    let collection = path.basename(file).replace(/-\d+\.[^\.]*$/, '')

    if (prev_collection !== collection) {
      await emit.collection(collection)
      prev_collection = collection
    }

    const stmt = db.prepare(`SELECT * FROM read_parquet($1)`)
    const lock = new Lock<void>()

    stmt.each(file, (_, row) => {
      emit.data(row)
    })

    stmt.finalize(() => {
      lock.resolve()
    })

    await lock.promise
    db.close()
  }

})
