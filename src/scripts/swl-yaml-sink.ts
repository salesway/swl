#!/usr/bin/env -S node --enable-source-maps

import { log2, sink, CollectionHandler, Sink, default_opts, } from "../index"
import { optparser, arg, flag, param } from "../optparse"
import { dump } from "js-yaml"

import * as fs from "fs"
import * as path from "path"

import { file } from "../debug"


let opts_parser = optparser(
  default_opts,
  arg("path").required().help("%.yml or collection.yml or empty string"),
  param("-d", "--delimiter").as("delimiter").default(";"),
  param("-q", "--quote").as("quote").default('"'),
  param("-m", "--multiple").as("multiple"),
  param("--charset").as("charset").default("utf-8"),
  flag("-n", "--no-headers").as("no_headers").default(false),
)

let opts = opts_parser.parse()

const cols: {[name: string]: any[]} = {}

function collection_handler(colname: string, start: any): CollectionHandler {

  const acc: any[] = []


  // writer.pipe(outfile)


  return {
    data(data) {
      acc.push(data)
    },
    end() {
      cols[colname] = acc
      // log2("opening", file(pathname), "for writing")
    }
  }
}

sink((): Sink => {

  const pathname = opts.path.endsWith(".yml") || opts.path.endsWith(".yaml") ? opts.path : opts.path.includes("%") ? opts.path.replace(/%/g, m => colname) : path.join(opts.path, colname + ".yml")

  return {
    collection(col, start) {
      return collection_handler(col.name, start)
    },
    error() {
      log2("error")
    },
    end() {
      const one_collection = Object.keys(cols).length === 1
      if (one_collection) {
        log2("opening", file(pathname), " for writing")
        fs.writeFileSync(pathname, dump(cols, { sortKeys: true }), "utf-8")
      } else {
        for (let x in cols) {

        }
      }

      log2("Finished")
    },
  }

})
