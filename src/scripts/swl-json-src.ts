#!/usr/bin/env -S bun run

import { readFileSync } from "fs"

import { arg, optparser, param } from "../optparse"
import { default_opts, emit, source } from '../index'
import { parse } from "json5"

let opts = optparser(
  param("-e", "--encoding").as("encoding"),
  param("-c", "--collection").as("collection"),
  arg("file").required(),
  default_opts,
)
  .parse()

source(function () {
  const source = opts.file
  const file_is_json = source[0] === "[" || source[0] === "{"

  let contents = file_is_json ? source : readFileSync(source, { encoding: (opts.encoding as BufferEncoding) ?? "utf-8" })

  let parsed = parse(contents)
  const default_collection = file_is_json ? "json" : source.replace(/.json$/, "")

  if (Array.isArray(parsed)) {
    parsed = { [opts.collection ?? default_collection]: parsed }
  } else if (file_is_json) {
    parsed = { [opts.collection ?? default_collection]: [parsed] }
  }

  for (const [col, cts] of Object.entries(parsed)) {

    emit.collection(col)

    for (var obj of cts as any[]) {
      emit.data(obj)
    }
  }

})