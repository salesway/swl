import * as fs from "fs"
import * as pth from "path"

import * as csv from "fast-csv"

import { emit, source } from ".."
import { optparser, arg, param, flag, } from "../optparse"

const opts_src = optparser(
  param("-d", "--delimiter").as("delimiter").default(","),
  param("-q", "--quote").as("quote"),
  flag("-n", "--no-empty").as("noempty"),
  flag("-N", "--empty-null").as("emptyisnull"),
  param("-e", "--escape").as("escape"),
  param("-h", "--headers").as("headers").default(""),
  param("-c", "--collection").as("collection"),
  param("-m", "--merge").as("merge").help("Add null columns"),
  flag("-s", "--simplify-headers").as("simplify_headers"),
  flag("-n", "--no-empty").as("noempty"),
  arg("files").required().repeat(),
)

const args = opts_src.parse()


source(async () => {

  for (let file of args.files) {
    let collection = args.collection ?? pth.basename(file).replace(/\.[^\.]*$/, '')
    let f = fs.createReadStream(file)
    // console.error(args)
    let opts: csv.ParserOptionsArgs = {
      delimiter: args.delimiter,
      objectMode: true,
      quote: args.quote ?? null,
      escape: args.escape ?? undefined,
      ignoreEmpty: true,
      discardUnmappedColumns: true,
    }

    if (args.headers) {
      opts.headers = args.headers.replace(/\n/g, ' ').split(/\s*,\s*/g)
      // console.log(opts.headers)
      opts.renameHeaders = true
    } else if (args.simplify_headers) {
      opts.headers = (h) => h.map(header => !header ? undefined : header!
        .toLowerCase()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
        .replace(/([^\w])+/g, '_').replace(/^_+|_+$/g, '')
        .trim()
      )
    } else {
      opts.headers = true
    }

    let stream = f.pipe(csv.parse(opts))
    let merge: any = null
    if (args.merge) {
      merge = args.merge.split(/\s*,\s*/g).reduce((acc, item) => ({[item]: null}), {} as any)
    }

    const noempty = !!args.noempty
    const emptyisnull = !!args.emptyisnull

    emit.collection(collection)
    for await (let line of stream) {
      if (merge) line = {...line, ...merge}
      if (noempty) {
        for (let x in line) {
          if (line[x] === "")
            delete line[x]
        }
      } else if (emptyisnull) {
        for (let x in line) {
          if (line[x] === "")
            line[x] = null
        }
      }
      emit.data(line)
    }
  }

})