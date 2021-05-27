#!/usr/bin/env -S node --enable-source-maps

import { sink, optparser } from "../index"
import { utils, writeFile } from "xlsx"

let opts = optparser()
  .arg("file")
  .flag("passthrough", {short: "p", long: "passthrough", help: "Let data flow to the next element"})
  .flag("compression", {short: "c", long: "compression", help: "Enable database compression"})
  .parse()

if (!opts.file) throw new Error(`xlsx needs a file argument`)

sink(function () {
  let wb = utils.book_new()

  return {
    collection(col) {
      let all_data: any[] = []

      return {
        data(data) {
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