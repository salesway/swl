#!/usr/bin/env -S node --enable-source-maps

import { readFileSync } from "fs"
import { createContext, runInContext } from "vm"

import { DEFAULT_SCHEMA, Type, load } from "js-yaml"

// there are unfortunately no typings for this library
const all = (require("js-yaml-js-types") as any).all

import { arg, optparser, param } from "../optparse"
import { default_opts, emit, source } from '../index'

let opts = optparser(
  arg("file").required(),
  param("-e", "--encoding").as("encoding"),
  default_opts,
)
  .parse()

source(function () {
  let contents = readFileSync(opts.file, opts.encoding ?? "utf-8")

  // A special context for the code being evaled by !!e
  let context = {}
  createContext(context)

  const schema = DEFAULT_SCHEMA.extend([
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
    ...all
  ])

  const parsed: object | any[] = load(contents, { filename: opts.file, schema }) as any

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