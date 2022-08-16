#!/usr/bin/env -S node --enable-source-maps

import { log2, log3, sink, CollectionHandler, Sink, default_opts, } from "../index"
import { optparser, arg, oneof, flag, param } from "../optparse"

import * as fs from "fs"
import * as path from "path"
import * as csv from "fast-csv"

import { file } from "../debug"


let opts_parser = optparser(
  default_opts,
  arg("path").required(),
  param("-d", "--delimiter").as("delimiter").default(";"),
  param("-q", "--quote").as("quote").default('"'),
  param("--charset").as("charset").default("utf-8"),
  flag("-n", "--no-headers").as("no_headers").default(false),
)

let opts = opts_parser.parse()


function collection_handler(colname: string, start: any): CollectionHandler {
  var columns = Object.keys(start)

  const writer = csv.format({
    alwaysWriteHeaders: !opts.no_headers,
    quote: opts.quote,
    delimiter: opts.delimiter,
    rowDelimiter: "\n",
    includeEndRowDelimiter: true,
    objectMode: true,
    escape: "\\",
    headers: columns,
  })

  const pathname = opts.path.includes(".csv") ? opts.path : opts.path.includes("%") ? opts.path.replace(/%/g, m => colname) : path.join(opts.path, colname + ".csv")
  log2("opening", file(pathname), "for writing")

  const outfile = fs.createWriteStream(pathname, opts.charset as "utf-8")
  writer.pipe(outfile)


  return {
    data(data) {
      // log2(data)
      writer.write(data)
    },
    end() {
      writer.end()
      // if (opts.verbose >= 2) {
      //   let s = db.prepare(`select count(*) as cnt from "${table}"`)
      //   log2("table", col_table(table), "now has", col_num(s.all()[0].cnt), "rows")
      // }
    }
  }
}

sink((): Sink => {
  return {
    collection(col, start) {
      return collection_handler(col.name, start)
    },
    error() {
      log2("error")
    },
    end() {
      log2("Finished")
    },
  }

})
