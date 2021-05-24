#!/usr/bin/env -S node --enable-source-maps

import { group, emit, util, flag, arg, parse_args, param, log } from 'swl'
import * as DB from 'better-sqlite3'

const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/
const re_number = /^\d+(\.\d+)?$/
const re_boolean = /^true|false$/i


export class SqliteSourceCollectionOptions {
  @arg name: string = ''
  @flag('u', {long: 'uncoerce'}) uncoerce = false
  @param('q', {long: 'query'}) query?: string
}

export class SqliteSourceOptions {
  @arg file: string = ''
  @flag('u', {long: 'uncoerce'}) uncoerce = false
  @group(SqliteSourceCollectionOptions) collections: SqliteSourceCollectionOptions[] = []

  post() {
    if (this.uncoerce) {
      for (let c of this.collections) c.uncoerce = true
    }
  }
}

let opts = parse_args(SqliteSourceOptions)
console.error(opts)
util.source(() => {
  let db = new DB(opts.file, {readonly: true, fileMustExist: true})
  var sources = opts.collections

  if (sources.length === 0) {
    // Auto-detect *tables* (not views)
    // If no sources are specified, all the tables are outputed.
    const st = db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
      .pluck()

    sources = st.all().map((name: string) => {
      let res = new SqliteSourceCollectionOptions()
      if (opts.uncoerce) res.uncoerce = true
      res.name = name
      return res
    })
  }

  for (var source of sources) {
    var sql = source.query ?? `SELECT * FROM "${source.name}"`

    var stmt = db.prepare(sql)

    emit.collection(source.name)
    // this.info(`Started ${colname}`)
    var iterator = (stmt as any).iterate() as IterableIterator<any>
    for (var s of iterator) {
      if (source.uncoerce) {
        var s2: any = {}
        for (var x in s)
          s2[x] = uncoerce(s[x])
        s = s2
      }

      emit.data(s)
    }
  }

})


/////////////////////////////////////////////////////

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
