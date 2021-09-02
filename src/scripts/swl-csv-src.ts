import * as fs from "fs"
import * as pth from "path"

import * as csv from "fast-csv"

import { emit, source } from ".."
import { optparser, arg, param, } from "../optparse"

const opts_src = optparser(
  param("-d", "--delimiter").as("delimiter").default(","),
  param("-q", "--quote").as("quote").default(`"`),
  param("-h", "--headers").as("headers").default(""),
  arg("files").required().repeat(),
)

const args = opts_src.parse()


source(async () => {

  for (let file of args.files) {
    let collection = pth.basename(file)
    let f = fs.createReadStream(file)
    let opts: csv.ParserOptionsArgs = { delimiter: args.delimiter, objectMode: true, quote: args.quote }
    if (args.headers) {
      opts.headers = args.headers.replace(/\n/g, ' ').split(/\s*,\s*/g)
      // console.log(opts.headers)
      opts.renameHeaders = true
    }

    let stream = f.pipe(csv.parse(opts))

    emit.collection(collection)
    for await (const line of stream) {
      emit.data(line)
    }
  }

})