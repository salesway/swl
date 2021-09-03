import * as fs from "fs"
import * as pth from "path"

import * as csv from "fast-csv"

import { emit, source } from ".."
import { optparser, arg, param, flag, } from "../optparse"

const opts_src = optparser(
  param("-d", "--delimiter").as("delimiter").default(","),
  param("-q", "--quote").as("quote"),
  param("-e", "--escape").as("escape"),
  param("-h", "--headers").as("headers").default(""),
  param("-c", "--collection").as("collection"),
  flag("-s", "--simplify-headers").as("simplify_headers"),
  arg("files").required().repeat(),
)

const args = opts_src.parse()


source(async () => {

  for (let file of args.files) {
    let collection = args.collection ?? pth.basename(file).replace(/\.[^\.]*$/, '')
    let f = fs.createReadStream(file)
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
        .replace(/([- ;,+'])+/g, '_')
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
      )
    } else {
      opts.headers = true
    }

    let stream = f.pipe(csv.parse(opts))

    emit.collection(collection)
    for await (const line of stream) {
      emit.data(line)
    }
  }

})