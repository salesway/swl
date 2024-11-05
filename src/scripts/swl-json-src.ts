#!/usr/bin/env -S bun run

import { readFileSync } from "fs"

import { arg, optparser, param } from "../optparse"
import { default_opts, emit, source } from '../index'

let opts = optparser(
  arg("file").required(),
  param("-e", "--encoding").as("encoding"),
  param("-c", "--collection").as("collection"),
  default_opts,
)
  .parse()

source(function () {
  let contents = readFileSync(opts.file, { encoding: (opts.encoding as BufferEncoding) ?? "utf-8" })
  let parsed = JSON.parse(contents)

  if (Array.isArray(parsed)) {
    parsed = { [opts.collection ?? opts.file.replace(/.json$/, "")]: parsed }
  }

  for (const [col, cts] of Object.entries(parsed)) {

    emit.collection(col)

    for (var obj of cts as any[]) {
      emit.data(obj)
    }
  }

})