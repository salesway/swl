#!/usr/bin/env node
import { source, emit, } from "../index"
import { optparser, arg, } from "../optparse"

import * as path from "path"
import * as fs from "fs"

import {
  tableFromIPC,
} from "apache-arrow"

import {
  readParquet,
} from "parquet-wasm/node/arrow2"

const opts_src = optparser(
  arg("files").required().repeat(),
)

let args = opts_src.parse()

source(async () => {
  const files = args.files.sort()
  let prev_collection = ""

  for (let file of files) {
    let collection = path.basename(file).replace(/-\d+\.[^\.]*$/, '')

    const buf = fs.readFileSync(file)
    const pqtbuf = readParquet(buf)
    const table = tableFromIPC(pqtbuf)

    if (prev_collection !== collection) {
      await emit.collection(collection)
      prev_collection = collection
    }

    const total = table.numRows
    const fields = table.schema.fields

    for (let i = 0; i < total; i++) {
      const obj: any = {}

      const item = table.get(i)
      for (let f of fields) {
        let v = (item as any)[f.name]
        if (Array.isArray(f.type.children)) {
          // ??
          v = [...v.toArray()]
        }
        if (typeof v === "bigint") {
          v = Number(v)
        }
        obj[f.name] = v
      }

      emit.data(obj)
    }
  }

})
