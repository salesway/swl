#!/usr/bin/env -S bun run

import { readFileSync } from "fs"
import { createContext, runInContext } from "vm"
import { basename } from "path"

import { DEFAULT_SCHEMA, Type, load } from "js-yaml"

// there are unfortunately no typings for this library
const all = (require("js-yaml-js-types") as any).all

import { arg, optparser, param } from "../src/optparse"
import { default_opts, emit, source } from '../src/index'

let opts = optparser(
  arg("file").required(),
  param("-e", "--encoding").as("encoding"),
  param("-c", "--collection").as("collection"),
  default_opts,
)
  .parse()

source(function () {
  let contents = readFileSync(opts.file, { encoding: (opts.encoding as BufferEncoding) ?? "utf-8" })

  // A special context for the code being evaled by !!e
  let context = {}
  createContext(context)
  const all2: Type[] = all.map((type: any) => new Type(type.tag, type.options))

  const types = [
    new Type('tag:yaml.org,2002:e', {
      kind: 'scalar',
      resolve: () => true,
      instanceOf: Object,
      construct: function (data) {
        let res = runInContext(data, context)
        // console.log(res)
        return res
      },
      // Not represented since there is no predicate.
      predicate: () => false,
      represent: () => undefined,
    }),
    ...all2
  ]

  const schema = DEFAULT_SCHEMA.extend(types)

  let parsed: object | any[] = load(contents, { filename: opts.file, schema }) as any
  if (Array.isArray(parsed)) {
    parsed = {[opts.collection ?? basename(opts.file)]: parsed}
  }

  var acc: {[name: string]: any[]} = {}
  for (const [col, cts] of Object.entries(parsed)) {
    if (col === '__refs__') {
      acc.__refs__ = cts
      continue
    }

    emit.collection(col)

    var _coll: any[] = acc[col] = []

    for (var obj of cts) {
      if (typeof obj === 'function') {
        const objs: any[] = []
        obj(acc, function (obj: any) { objs.push(obj) })
        if (objs.length) {
          for (var ob of objs) {
            _coll.push(ob)
            const { __meta__, ...to_send } = ob
            emit.data(to_send)
          }
        }
      } else {
        _coll.push(obj)
        const { __meta__, ...ob } = obj
        emit.data(ob)
      }
    }
  }

})