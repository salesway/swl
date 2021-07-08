#!/usr/bin/env -S node --enable-source-maps

import { default_opts, sink } from "../index"
import { utils, writeFile } from "xlsx"

import { arg, flag, optparser } from "../optparse"


let opts = optparser(
  arg("file").required(),
  default_opts,
  flag("-c", "--compress").as("compression").help("Enable XLSX compression"),
)
  .parse()

if (!opts.file) throw new Error(`xlsx needs a file argument`)

sink(function () {
  let wb = utils.book_new()

  return {
    collection(col) {
      let all_data: any[] = []

      return {
        data(data) {
          for (let x in data) {
            let val = data[x]
            if (val instanceof Date) data[x] = val.toISOString()
            else if (val != null && typeof val === "object") data[x] = JSON.stringify(val)
          }
          all_data.push(data)
        },
        end() {
          utils.book_append_sheet(
            wb,
            utils.json_to_sheet(all_data),
            col.name
          )
        }
      }
    },
    end() {
      writeFile(wb, opts.file, { compression: !!opts.compression, })
    }
  }
})