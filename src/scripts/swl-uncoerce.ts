#!/usr/bin/env -S node --enable-source-maps

import { emit, sink } from "../index"
import { optparser, param, flag } from "../optparse"

const opts_src = optparser(
  param("-o", "--only-columns").as("only"),
  param("-e", "--execpt").as("except"),
  flag("-t", "--trim").as("trim"),
  flag("-n", "--empty-is-null").as("null")
)

const opts = opts_src.parse()

let only: null | Set<string> = null
let except: null | Set<string> = null

if (opts.only) {
  only = new Set(opts.only.split(/[\n\s]*,[\s\n]*/g))
}
if (opts.except) {
  except = new Set(opts.except.split(/[\n\s]*,[\s\n]*/g))
}


sink(function () {
  return {
    collection(col) {
      let name = col.name
      let coerce_null = opts.null
      emit.collection(name)

      return {
        data(data) {
          let res: {[name: string]: any} = {}
          for (let x in data) {
            let dt = data[x]
            res[x] = (!only || only.has(x)) && (!except || !except.has(x)) ?
              (coerce_null && dt === "" ? null : uncoerce(dt))
            : dt
          }
          emit.data(res)
        },
        end() {
          /* do nothing */
        }
      }
    },
    end() { /* do nothing */ }
  }
})

const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/i
const re_number = /^\d+(\.\d+)?$/i
const re_boolean = /^true|false$/i
const re_default_null = /^null$/i

export function uncoerce(value: any) {
  if (value && (value[0] === '{' || value[0] === '[')) {
    try {
      return JSON.parse(value)
    } catch (e) {
      return value
    }
  }

  if (typeof value === 'string') {
    var trimmed = value.trim()

    if (trimmed.match(re_date)) {
      return new Date(trimmed)
    }

    if (trimmed.match(re_boolean)) {
      return trimmed.toLowerCase() === 'true'
    }

    if (trimmed.match(re_number)) {
      return parseFloat(trimmed)
    }

    if (trimmed.match(re_default_null))
      return null

    if (opts.trim)
      return trimmed
  }


  return value
}
