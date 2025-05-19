#!/usr/bin/env -S bun run

import { log2, sink, Sink, default_opts, } from "../src/index"
import { optparser, arg, flag, } from "../src/optparse"

import * as fs from "fs"
import * as path from "path"



let opts_parser = optparser(
  default_opts,
  arg("path").required().help("%.json or collection.json or directory or empty string"),
  flag("--object", "-o").as("obj").help("output as object of collections instead of array"),
)

let opts = opts_parser.parse()


sink((): Sink => {

  if (!opts.path) {
    opts.path = process.cwd()
  }

  const stat = fs.statSync(opts.path, { throwIfNoEntry: false })
  const _w = stat?.isDirectory() || opts.path.includes("%") ? null : fs.createWriteStream(opts.path, {  })


  if (_w) {
    if (opts.obj) {
      _w.write("{\n")
    }
  }

  let _first = true

  return {
    collection(col, start) {
      const col_path = stat?.isDirectory() ? path.join(opts.path, col.name + ".json") : opts.path.replace(/%/, col.name)
      const w = _w ?? fs.createWriteStream(col_path)
      let first = true

      if (!_first) {
        w.write(",")
      } else {
        _first = false
      }
      w.write(opts.obj ? `"${col.name}": [\n` : "[\n")

      return {
        data(data: any) {
          if (first) {
            _first = first = false
          } else {
            w.write(",\n")
          }
          w.write(JSON.stringify(data))
        },
        end () {
          w.write("\n]")
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

      if (opts.obj) {
        _w?.write("}")
      }
      _w?.close()
      log2("Finished")
    },
  }

})
