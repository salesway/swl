#!/usr/bin/env -S bun run

import { CollectionHandler, log, sink, uri_maybe_open_tunnel, Lock, Collection, log2, col_sink, default_opts, log1, log3, col_num, col_table, col_updated } from '../src/index'
import { optparser, arg, flag, param, oneof } from "../src/optparse"

import { Client as PgClient, types } from 'pg'
import { from as copy_from } from 'pg-copy-streams'
import * as json from "json-bigint"

let col_options = optparser(
  flag("-n", "--table-name").as("table_name").help("Specify a different table name than the collection name"),
  flag("-a", "--auto-create").as("auto_create").help("Create table if it didn't exist"),
  flag("-t", "--truncate").as("truncate"),
  flag("-d", "--drop").as("drop"),
  flag("-u", "--upsert").as("upsert"),
  flag("--do-nothing").as("do_nothing"),
  flag("-U", "--update").as("update"),
)

let col_parser = optparser(
  arg("name"),
  col_options
)

let opts_parser = optparser(
  arg("uri").required().help("a postgres uri to connect to"),
  flag("-a", "--auto-create").as("auto_create").default(false).help("Create table if it didn't exist"),
  default_opts,

  flag("--disable-triggers").as("disable_triggers").help("Disable triggers before loading data"),
  flag("-n", "--notice").as("notice").help("Display NOTICE statements"),
  flag("-y", "--notify").as("notify").help("Display LISTEN/NOTIFY requests"),
  flag("--ignore-non-existing").as("ignore_nonexisting").help("Ignore tables that don't exist"),
  param("-s", "--schema").as("schema").default("public").help("Default schema to analyze when no collections specified"),
  col_options,
  oneof(col_parser).as("collections").repeat(),
)

let opts = opts_parser.parse()

for (let c of opts.collections) {
  if (opts.truncate) c.truncate = true
  if (opts.drop) c.drop = true
  if (opts.upsert) c.upsert = opts.upsert
  if (opts.update) c.update = true
  if (opts.auto_create) c.auto_create = true
}

const colls_options = new Map(opts.collections.map(c => [c.name, c]))


// Date type, don't remember if this is essential or not.
types.setTypeParser(1082, val => {
  // var d = new Date(val)
  return val
})


sink(async () => {
  log2("connecting to database", col_sink(opts.uri))
  let open = await uri_maybe_open_tunnel(opts.uri, 5432)
  let uri = open.uri.startsWith("postgres://") ? open.uri : `postgres://${open.uri}`

  let db = new PgClient(uri)
  const seen_collections = new Set<string>()

  return {

    // Setup the database and some global options, such as displaying notices
    async init() {
      await db.connect()
      log2("connected")

      // Display notice
      if (opts.notice) {
        db.on("notice", notice => {
          log(`NOTICE ${notice.severity}: ${notice.message}`)
        })
      }

      // Display notify
      if (opts.notify) {
        db.on("notification", notification => {
          log(`NOTIFY ${notification.channel}:${notification.processId}: `, notification.payload)
        })
      }

      // Disable triggers
      if (opts.disable_triggers) {
        log1("disabling triggers")
        await db.query(/* sql */ `SET session_replication_role = replica;`)
      }

      await db.query("BEGIN")
    },

    async collection(col, first) {
      return collection_handler(db, col, first, seen_collections)
    },

    async end() {
      await db.query("COMMIT")
      log2("commited changes")
    },

    async error() {
      await db.query("ROLLBACK")
      log2("rolled back changes")
    },

    async finally() {
      log2("running analyze")
      await db.query("ANALYZE")
      log2("disconnecting from database")
      await db.end()
    }
  }
})

async function collection_handler(db: PgClient, col: Collection, first: any, seen: Set<string>): Promise<CollectionHandler> {

  const table = col.name
  const temp_table_name = `${table.replace('.', '__')}_temp`
  const columns = Object.keys(first)
  let hstore_columns: string[] | undefined = undefined
  // var types = columns.map(c => typeof first[c] === 'number' ? 'real'
  // : first[c] instanceof Date ? 'timestamptz'
  // : first[c] instanceof Buffer ? 'blob'
  // : 'text')
  // console.log(chunk.collection, types)

  // Figure out if the input name is dotted or not. If not, then use "public" ?
  let schema = opts.schema
  let table_name = table
  if (table.includes("."))
    [schema, table_name] = table.split(".")

  async function Q(sql: string, args?: any[]) {
    log3(sql)
    return await db.query(sql, args)
  }


  const col_opts = colls_options.get(col.name)

  const options = {
    upsert: col_opts?.upsert || opts.upsert,
    do_nothing: col_opts?.do_nothing || opts.do_nothing,
    update: col_opts?.update || opts.update,
    auto_create: col_opts?.auto_create || opts.auto_create,
    drop: col_opts?.drop || opts.drop,
    truncate: col_opts?.truncate || opts.truncate,
  }

  if (!seen.has(col.name)) {

    if (options.drop) {
      log(`dropping ${col_table(table)}`)
      await Q(/* sql */`DROP TABLE IF EXISTS ${table}`)
    }

    // Create the table if it didn't exist
    if (options.auto_create) {
      log(`creating ${col_table(table)}`)
      await Q(/* sql */`
        CREATE TABLE IF NOT EXISTS ${table} (
          ${columns.map((c, i) => `"${c}" text`).join(', ')}
        )
      `)
    }

    if (options.truncate) {
      const res = await Q(/* sql */`TRUNCATE ${table} RESTART IDENTITY  CASCADE`)
      log(`${col_table(table)}: ${col_num(res.rowCount)} rows truncated`)
    }

    seen.add(col.name)
  }
  // Create a temporary table that will receive all the data through pg COPY
  // command. This table will hold plain json objects
  // log2("Creating temp table", temp_table_name)
  await Q(/* sql */`
    CREATE TEMP TABLE ${temp_table_name} (
      jsondata json
    )
  `)

  // Figure out if we have some hstore columns, which will need to be rebuilt
  let hstore_columns_query = await Q(/* sql */`
    SELECT
      json_agg(column_name) as hstore_columns
    FROM information_schema.columns
    WHERE table_name = $2 AND table_schema = $1 AND udt_name = 'hstore'
  `, [schema, table_name])

  hstore_columns = hstore_columns_query.rows[0]?.hstore_columns

  // We create a copy stream to the database where we will dump exactly one JSON
  // object per line, using @ as the quote character, which we double in the stream input.
  // This is because we will then use the fantastic json_populate_record to
  // actually create the rows when inserting.
  // Which means that old versions of postgres are *not* supported by this sink.
  let stream: NodeJS.WritableStream = await db.query(
    copy_from(/* sql */`COPY ${temp_table_name}(jsondata) FROM STDIN
      WITH
      (NULL '**NULL**', DELIMITER '|', FORMAT csv, QUOTE '@')`
  )) as any

  let drain_lock: Lock<void> | null = null
  stream.on("drain", _ => {
    drain_lock?.resolve()
  })
  stream.on("finish", _ => {
    log3("stream ended")
  })

  const hstore_len = hstore_columns?.length ?? 0
  function hstore_quote(s: string) { return s.replace(/["\\]/g, m => "\\" + m) }

  return {
    async data(data) {
      if (hstore_len) {
        for (let i = 0; i < hstore_len; i++) {
          const col = hstore_columns![i]
          const dt = data[col]
          if (dt == null || typeof data[hstore_columns![i]] === "string") continue
          // transform the object into an hstore compliant string
          data[col] = Object.getOwnPropertyNames(dt)
            .map(name => `"${hstore_quote(name)}"=>"${hstore_quote(dt[name])}"`).join(",")
          // console.error(data[col])
        }
      }
      const payload = '@' + json.stringify(data).replace(/@/g, '@@') + '@\n'
      // console.error(payload)
      if (!stream.write(payload)) {
        drain_lock = new Lock()
        await drain_lock.promise
        drain_lock = null
      }
    },
    async end() {
      // log2("closing COPY pipe")
      stream.end()

      // If we're trying to UPSERT data instead of plain INSERT, figure out the
      // primary key constraint name for this table, and use it.
      // Note: we should be able to specify the constraint manually, maybe not just the primary key ?
      let upsert = ""
      if (col_opts?.upsert || !!opts.upsert || opts.do_nothing) {
        var cst = (await Q(/* sql */`
          SELECT
            *
          FROM information_schema.table_constraints
          WHERE table_name = '${table_name}' AND constraint_schema = '${schema}'
            AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE')
          ORDER BY constraint_type
        `))

        // console.log(cst.rows)
        if (cst.rows.length > 0) {
          upsert = /* sql */ ` ON CONFLICT ON CONSTRAINT "${typeof opts.upsert === "string" ? opts.upsert : cst.rows[0].constraint_name}" ${opts.do_nothing ? `DO NOTHING` : `DO UPDATE SET ${columns.map(c => `"${c}" = EXCLUDED."${c}"`)} `}`
        }

      }

      // Now we have enough information to actually perform the INSERT statement
      // log2("Inserting data into", table, "from temp table")

      if (opts.update) {
        let pk: {rows: {cols: string[]}[]} = (await Q(/* sql */`
          SELECT json_agg(a.attname) as cols
          FROM   pg_index i
          JOIN   pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
          WHERE  i.indrelid = '${table}'::regclass
          AND    i.indisprimary;
        `))
        let pk_columns = pk.rows[0].cols

        const res = await Q(/* sql */`
          UPDATE ${table}
            SET ${columns.filter(col => pk_columns.indexOf(col) === -1)
              .map(col => `"${col}" = (T.rec)."${col}"`)
              .join(", ")
            }
          FROM (
            SELECT json_populate_record(null::${table}, T.jsondata) rec FROM ${temp_table_name} T
          ) T
          WHERE ${pk_columns.map(col => `(T.rec)."${col}" = ${table}."${col}"`).join(" AND ")}
        `)
        log(`${col_table(table)}: ${col_num(res.rowCount)} rows updated`)
        // console.error(res)
        // console.error(pk.rows[0].cols)
        // update = /* sql */ `UPDATE ${table}(${})`
      } else {
        const res = await Q(/* sql */`
          WITH upsert AS (INSERT INTO ${table}(${columns.map(c => `"${c}"`).join(', ')}) (
            SELECT ${columns.map(c => 'R."' + c + '"').join(', ')}
            FROM ${temp_table_name} T,
              json_populate_record(null::${table}, T.jsondata) R
          )
          ${upsert}
          RETURNING xmax = 0 as inserted)
          SELECT
            COUNT(*) FILTER (WHERE inserted) AS num_inserted,
            COUNT(*) FILTER (WHERE NOT inserted) AS num_updated
          FROM upsert
        `)
        const rows = res.rows[0]
        if (options.upsert) {
          log(`${col_table(table)}: ${rows.num_inserted > 0 ? col_num(rows.num_inserted) : "no"} rows inserted, ${rows.num_updated > 0 ? col_updated(rows.num_updated) : "no"} rows updated`)
        } else {
          log(`${col_table(table)}: ${col_num(rows.num_inserted)} rows inserted`)
        }
      }

      // Drop the temporary table
      await Q(/* sql */`
        DROP TABLE ${temp_table_name}
      `)

      // The following instructions are used to reset sequences if we have have forced
      // id blocks.
      // First, we get all the sequences associated with this table
      const seq_res = await Q(/* sql */`
        SELECT
          column_name as name,
          regexp_replace(
            regexp_replace(column_default, '[^'']+''', ''),
            '''.*',
            ''
          ) as seq
        FROM information_schema.columns
        WHERE table_name = '${table_name}'
          AND table_schema = '${schema}'
          AND column_default like '%nextval(%'
      `)

      const sequences = seq_res.rows as {name: string, seq: string}[]

      for (var seq of sequences) {
        log2(`Resetting sequence ${seq.seq}`)
        await Q(/* sql */`
          DO $$
          DECLARE
            themax INT;
          BEGIN
            LOCK TABLE ${table} IN EXCLUSIVE MODE;
            SELECT MAX(${seq.name}) INTO themax FROM ${table};
            PERFORM SETVAL('${seq.seq}', COALESCE(themax + 1, 1), false);
          END
          $$ LANGUAGE plpgsql;
        `)
      }

    }
  }
}
