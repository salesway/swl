#!/usr/bin/env -S bun run

import { log2, sink, Sink, default_opts, } from "../index"
import { optparser, arg, } from "../optparse"

import * as fs from "fs"
import * as path from "path"


let opts_parser = optparser(
  default_opts,
  arg("path").required().help("%.yml or collection.yml or empty string"),
)

let opts = opts_parser.parse()


sink((): Sink => {

  if (!opts.path) {
    opts.path = process.cwd()
  }
  const stat = fs.statSync(opts.path, { throwIfNoEntry: false })
  const _w = stat?.isDirectory() || opts.path.includes("%") ? null : fs.createWriteStream(opts.path, {  })

  return {
    collection(col, start) {
      if (_w != null) {
        _w.write(col.name + ":\n")
      }

      const col_path = stat?.isDirectory() ? path.join(opts.path, col.name + ".json") : opts.path.replace(/%/, col.name)
      const w = _w ?? fs.createWriteStream(col_path)

      return {
        data(data: any) {
          w.write(`- ${JSON.stringify(data)}\n`)
        },
        end() {
          if (_w == null) {
            w.close()
          }
        }
      }
    },
    error() {
      log2("error")
    },
    end() {
      _w?.close()
      log2("Finished")
    },
  }

})
