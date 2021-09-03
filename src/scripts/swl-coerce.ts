#!/usr/bin/env -S node --enable-source-maps

import { sink, emit } from '../index'

import { optparser, param, } from "../optparse"

const opts_src = optparser(
  param("-o", "--only-columns").as("only"),
)

const opts = opts_src.parse()

let only: null | Set<string> = null

if (opts.only) {
  only = new Set(opts.only.split(/[\n\s]*,[\s\n]*/g))
}

sink(function () {
  return {
    collection(col) {
      let name = col.name
      emit.collection(name)

      return {
        data(data) {
          let res: {[name: string]: any} = {}
          for (let x in data) {
            res[x] = !only || only.has(x) ? coerce(data[x]) : data[x]
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

export function coerce(value: any) {
  const typ = typeof value
  if (value === null || typ === 'string' || typ === 'number' || value instanceof Buffer) {
    return value
  }
  if (typ === 'boolean')
    return value ? 'true' : 'false'
  if (value === undefined)
    return null

  if (value instanceof Date) {
    return (new Date(value.valueOf() - (value.getTimezoneOffset() * 60000))).toISOString()
  } //if (Array.isArray(value))
    //return value.join(', ')

  return JSON.stringify(value)
}
