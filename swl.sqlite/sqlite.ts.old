
import {Sequence, OPT_OBJECT, URI, s, Chunk, Source, register, ParserType, Sink } from 'swl'
import * as S from 'better-sqlite3'


function coalesce_join(sep: string, ...a: (string|null|number)[]) {
  var r = []
  var l = a.length
  for (var i = 0; i < l; i++) {
    var b = ('' + (a[i]||'')).trim()
    if (b) r.push(b)
  }
  return r.join(sep)
}

function cleanup(str: string) {
  return (str||'').trim()
    .replace(/\s+/g, ' ')
    .replace(/\s*-\s*/g, '-')
}

var cache: {[name: string]: number} = {}
function counter(name: string, start: number) {
  var res = cache[name] = (cache[name] || start) + 1
  return res
}

function reset_counter(name: string) {
  delete cache[name]
}


const SQLITE_SOURCE_OPTIONS = s.object({
  uncoerce: s.boolean(false)
})

const SQLITE_BODY = Sequence(URI, OPT_OBJECT).name`SQlite Options`

@register('sqlite', 'sqlite3', '.db', '.sqlite', '.sqlite3')
export class SqliteSource extends Source<
  s.BaseType<typeof SQLITE_SOURCE_OPTIONS>,
  ParserType<typeof SQLITE_BODY>
  >
{
  help = `Read an SQLite database`

  options_parser = SQLITE_SOURCE_OPTIONS
  body_parser = SQLITE_BODY

  // ????
  uncoerce!: boolean
  filename!: string
  sources!: {[name: string]: boolean | string}

  db!: S

  async init() {
    this.filename = await this.body[0]
    this.sources = this.body[1]
    this.uncoerce = this.options.uncoerce

    this.db = new S(this.filename, {readonly: true, fileMustExist: true})

    this.db.function('coalesce_join', {
      varargs: true, deterministic: true, safeIntegers: true}, coalesce_join)
    this.db.function('cleanup', {
      varargs: false,
      deterministic: true,
      safeIntegers: true}, cleanup)
    this.db.function('counter', {
      varargs: false,
      deterministic: false,
      safeIntegers: true}, counter)
    this.db.function('reset_counter', {
      varargs: false,
      deterministic: false, safeIntegers: true}, reset_counter)
  }

  async end() {
    this.db.close()
  }

  async emit() {
    var sources = this.sources
    var keys = Object.keys(sources||{})

    if (keys.length === 0) {
      // Auto-detect *tables* (not views)
      // If no sources are specified, all the tables are outputed.
      const st = this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`)
        .pluck<string>()
      keys = st.all()
    }

    for (var colname of keys) {
      var val = sources[colname]

      var sql = typeof val !== 'string' ? `SELECT * FROM "${colname}"`
      : !val.trim().toLowerCase().startsWith('select') ? `SELECT * FROM "${val}"`
      : val

      var stmt = this.db.prepare(sql)

      this.info(`Started ${colname}`)
      var iterator = (stmt as any).iterate() as IterableIterator<any>
      for (var s of iterator) {
        if (this.uncoerce) {
          var s2: any = {}
          for (var x in s)
            s2[x] = uncoerce(s[x])
          s = s2
        }

        await this.send(Chunk.data(colname, s))
      }
    }
    this.info('done')

  }

}

export const SQLITE_SINK_OPTIONS = s.object({
  truncate: s.boolean(false),
  drop: s.boolean(false),
  pragma: s.boolean(true)
})

export const SQLITE_SINK_BODY = URI

@register('sqlite', '.db', '.sqlite', '.sqlite3')
export class SqliteSink extends Sink<
  s.BaseType<typeof SQLITE_SINK_OPTIONS>,
  ParserType<typeof URI>
> {

  help = `Write to a SQLite database`
  options_parser = SQLITE_SINK_OPTIONS
  body_parser = URI

  mode = 'insert' as 'insert' | 'upsert' | 'update'

  table = ''
  db!: S

  pragmas: {[name: string]: any} = {}
  columns: string[] = []
  stmt: {run(...a:any): any} = undefined!

  async init() {
    const db = new S(await this.body, {})
    this.db = db

    if (this.options.pragma) {
      this.pragmas.journal_mode = db.pragma('journal_mode', true)
      this.pragmas.synchronous = db.pragma('synchronous', true)
      this.pragmas.locking_mode = db.pragma('locking_mode', true)

      db.pragma('journal_mode = off')
      db.pragma('synchronous = 0')
      db.pragma('locking_mode = EXCLUSIVE')
    }

    db.exec('BEGIN')
  }

  async error(e: any) {
    this.db.exec('ROLLBACK')
    throw e
  }

  async end() {
    this.db.exec('COMMIT')
  }

  async final() {
    if (this.options.pragma) {
      for (var x in this.pragmas) {
        this.db.pragma(`${x} = ${this.pragmas[x]}`)
      }
    }
    if (this.db) this.db.close()
  }


  /**
   * Create the table, truncate it or drop it if necessary
   */
  async onCollectionStart(start: Chunk.Data) {

    var sql = ''
    var table = start.collection
    var payload = start.payload
    var columns = Object.keys(payload)
    this.columns = columns

    var types = columns.map(c => typeof payload[c] === 'number' ? 'int'
    : payload[c] instanceof Buffer ? 'blob'
    : 'text')

    if (this.options.drop) {
      sql = `DROP TABLE IF EXISTS "${table}"`
      this.info(sql)
      this.db.exec(sql)
    }

    // Create if not exists ?
    // Temporary ?
    sql = `CREATE TABLE IF NOT EXISTS "${table}" (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )`
    this.info(sql)
    this.db.exec(sql)

    if (this.mode === 'insert') {
      const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
      values (${columns.map(c => '?').join(', ')})`
      // console.log(sql)
      this.stmt = this.db.prepare(sql)
    }

    else if (this.mode === 'upsert')
      // Should I do some sub-query thing with coalesce ?
      // I would need some kind of primary key...
      this.stmt = this.db.prepare(`INSERT OR REPLACE INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')})
        values (${columns.map(c => '?').join(', ')})`)


    if (this.options.truncate) {
      sql = `DELETE FROM "${table}"`
      this.info(sql)
      this.db.exec(sql)
    }
  }

  async onData(data: Chunk.Data) {
    this.stmt.run(...this.columns.map(c => coerce(data.payload[c])))
  }
}


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
