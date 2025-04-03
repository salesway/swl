#!/usr/bin/env -S bun run

import { default_opts, log, sink } from "../src/index"
import { utils, writeFile, readFile } from "xlsx"

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
  try {
    // try to load if it exists
    wb = readFile(opts.file)
    log("opened existing file")
  } catch { }

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
          if (wb.Sheets[col.name]) {
            delete wb.Sheets[col.name]
            wb.SheetNames = wb.SheetNames.filter(s => s !== col.name)
          }
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