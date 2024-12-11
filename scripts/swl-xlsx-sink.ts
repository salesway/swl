#!/usr/bin/env -S bun run

import { default_opts, sink } from "../src/index"
import { utils, writeFile } from "xlsx"

import { arg, flag, optparser } from "../src/optparse"


let opts = optparser(
  arg("file").required(),
  default_opts,
  flag("-u", "--uncompress").as("uncompress").help("Disable XLSX compression"),
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
      writeFile(wb, opts.file, { compression: !opts.uncompress, })
    }
  }
})