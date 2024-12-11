#!/usr/bin/env -S bun run

import { log2, sink, Sink, default_opts, } from "../src/index"
import { optparser, arg, } from "../src/optparse"

import * as fs from "fs"
import * as path from "path"



let opts_parser = optparser(
  default_opts,
  arg("path").required().help("%.json or collection.json or directory or empty string"),
)

let opts = opts_parser.parse()


sink((): Sink => {

  if (!opts.path) {
    opts.path = process.cwd()
  }
  const stat = fs.statSync(opts.path, { throwIfNoEntry: false })
  const _w = stat?.isDirectory() || opts.path.includes("%") ? null : fs.createWriteStream(opts.path, {  })
  if (_w) {
    _w.write("[\n")
  }
  let _first = true

  return {
    collection(col, start) {
      const col_path = stat?.isDirectory() ? path.join(opts.path, col.name + ".json") : opts.path.replace(/%/, col.name)
      const w = _w ?? fs.createWriteStream(col_path)
      let first = _w ? _first : true

      return {
        data(data: any) {
          if (first) {
            _first = first = false
            if (_w == null) { w.write("[\n") }
          } else {
            w.write(",\n")
          }
          w.write(JSON.stringify(data))
        },
        end () {
          if (_w == null) {
            w.write("\n]")
            w.close()
          }
        }
      }

    },
    error() {
      log2("error")
    },
    end() {
      _w?.write("]")
      _w?.close()
      log2("Finished")
    },
  }

})
