import * as ch from 'chalk'
import { col_error } from '.'
import { Chunk, ChunkType, chunk_is_collection, chunk_is_data, chunk_is_error, ErrorChunk } from './types'

export const c = new ch.Instance({level: 3})
// const c = ch.constructor({level: 3})
export const file = c.hsl(20, 40, 40)
export const grey = c.rgb(111, 111, 111)
export const constant = c.hsl(0, 60, 60)
export const info = c.hsl(40, 60, 60)
export const str = c.hsl(80, 60, 60)
export const num = c.hsl(120, 60, 60)
export const date = c.hsl(140, 60, 60)
export const prop = c.hsl(180, 30, 30)
export const coll = c.hsl(220, 60, 60)
export const bool = c.hsl(280, 60, 60)



const fmt = Intl.DateTimeFormat('fr', {
  hour12: false,
  year: 'numeric',
  month: 'numeric',
  day: 'numeric',
  hour: 'numeric',
  minute: 'numeric',
  second: 'numeric'
})

export function print_value(out: NodeJS.WritableStream, obj: any, outside = true) {

  if (obj == null) {
    out.write(constant(obj))
  } else if (typeof obj === 'string') {
    out.write(str(obj.replace(/\n/g, '\\n') || "''"))
  } else if (typeof obj === 'number') {
    out.write(num(obj as any))
  } else if (typeof obj === 'boolean') {
    out.write(bool(obj as any))
  } else if (obj instanceof Date) {
    out.write(date(fmt.format(obj)))
  } else if (obj instanceof Array) {
    out.write('[')
    var first = true
    for (var e of obj) {
      if (!first) out.write(', ')
      print_value(out, e, false)
      first = false
    }
    out.write(']')
  } else if (typeof obj === 'object') {
    if (!outside)
      out.write('{')
    var first = true
    for (var x in obj) {
      if (!first) out.write(prop(', '))
      out.write(prop(x + ': '))
      print_value(out, obj[x], false)
      first = false
    }
    if (!outside)
      out.write('}')
  } else {
    out.write(obj)
  }
}

var _current_collection = ''
var _current_nb = 0

export function debug(type: ChunkType, chunk: Chunk) {
  if (chunk_is_collection(type, chunk)) {
    _current_collection = chunk.name
    _current_nb = 0
  } else if (chunk_is_data(type, chunk)) {
    // var d = chunk
    process.stderr.write(`${info(_current_collection)}:${num(++_current_nb)} `)
    print_value(process.stderr, chunk)
    process.stderr.write('\n')
  } else if (chunk_is_error(type, chunk)) {
    const _ = chunk as ErrorChunk
    process.stderr.write(_.origin + " " + col_error("ERROR") + " " + _.message + " ")
    process.stderr.write('\n')
    if (_.payload) {
      let pay = _.payload
      try {
        pay = JSON.parse(_.payload)
      } catch { }

      for (let x in pay) {
        process.stderr.write(_.origin + " " + col_error(x) + ": " + pay[x] + "\n")
      }
    } else {

    }
  } else {
    console.error(chunk)
  }
}
