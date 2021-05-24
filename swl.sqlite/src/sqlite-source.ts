#!/usr/bin/env -S node --enable-source-maps

import { group, emit, util, flag, arg, parse_args, param } from 'swl'
import * as DB from 'better-sqlite3'

import { uncoerce } from './common'

export class SqliteSourceCollectionOptions {
  @arg name: string = ''
  @flag('u', {long: 'uncoerce'}) uncoerce: boolean = false
  @param('q', {long: 'query'}) query?: string
}

export class SqliteSourceOptions {
  @arg file: string = ''
  @flag('u', {long: 'uncoerce'}) uncoerce: boolean = false
  @group(SqliteSourceCollectionOptions) collections: SqliteSourceCollectionOptions[] = []

  post() {
    if (this.uncoerce) {
      for (let c of this.collections) c.uncoerce = true
    }
    if (!this.file) throw new Error('sqlite source expects a file name')
  }
}

let opts = parse_args(SqliteSourceOptions)

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

