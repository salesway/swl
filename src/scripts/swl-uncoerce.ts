#!/usr/bin/env -S node --enable-source-maps

import { emit, sink } from "../index"

sink(function () {
  return {
    collection(col) {
      let name = col.name
      emit.collection(name)

      return {
        data(data) {
          let res: {[name: string]: any} = {}
          for (let x in data) {
            res[x] = uncoerce(data[x])
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

const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/
const re_number = /^\d+(\.\d+)?$/
const re_boolean = /^true|false$/i

export function uncoerce(value: any) {
  if (value && (value[0] === '{' || value[0] === '[')) {
    try {
      return JSON.parse(value)
    } catch (e) {
      return value
    }
  }

  if (typeof value === 'string') {
    var trimmed = value.trim().toLowerCase()

    if (trimmed.match(re_date)) {
      return new Date(trimmed)
    }

    if (trimmed.match(re_boolean)) {
      return trimmed.toLowerCase() === 'true'
    }

    if (trimmed.match(re_number)) {
      return parseFloat(trimmed)
    }

    if (trimmed === 'null')
      return null
  }

  return value
}
