#!/usr/bin/env -S node --enable-source-maps

import { CollectionHandler, log, sink, uri_maybe_open_tunnel, Lock, Collection, log2, col_sink, default_opts } from '../index'
import { optparser, arg, flag, param, oneof } from "../optparse"

import { Client as PgClient, types } from 'pg'
import { from as copy_from } from 'pg-copy-streams'

let col_options = optparser(
  flag("-n", "--table-name").as("table_name").help("Specify a different table name than the collection name"),
  flag("-a", "--auto-create").as("auto_create").help("Create table if it didn't exist"),
  flag("-t", "--truncate").as("truncate"),
  flag("-d", "--drop").as("drop"),
  flag("-u", "--upsert").as("upsert"),
)

let col_parser = optparser(
  arg("name"),
  col_options
)

let opts_parser = optparser(
  arg("uri").required(),
  default_opts,
  col_options,

  flag("-t", "--disable-triggers").as("disable_triggers").help("Disable triggers before loading data"),
  flag("-n", "--notice").as("notice").help("Display NOTICE statements"),
  flag("-y", "--notify").as("notify").help("Display LISTEN/NOTIFY requests"),
  flag("-i", "--ignore-non-existing").as("ignore_nonexisting").help("Ignore tables that don't exist"),
  param("-s", "--schema").as("schema").default("public").help("Default schema to analyze when no collections specified"),
  flag("-p", "--passthrough").as("passthrough").help("Forward all data to the next pipe"),
  oneof(col_parser).as("collections").repeat(),
)


let opts = opts_parser.parse()

for (let c of opts.collections) {
  if (opts.truncate) c.truncate = true
  if (opts.drop) c.drop = true
  if (opts.upsert) c.upsert = true
  if (opts.auto_create) c.auto_create = true
}


// Date type, don't remember if this is essential or not.
types.setTypeParser(1082, val => {
  // var d = new Date(val)
  return val
})


sink(async () => {
  log2("connecting to database", col_sink(opts.uri))
  let open = await uri_maybe_open_tunnel(opts.uri)
  let uri = open.uri.startsWith("postgres://") ? open.uri : `postgres://${open.uri}`

  let db = new PgClient(uri)

  return {
    passthrough: !!opts.passthrough,

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
      if (opts.disable_triggers)
        await db.query(/* sql */ `SET session_replication_role = replica;`)

    },

    async collection(col, first) {
      return collection_handler(db, col, first)
    },

    async end() {
      await db.query("COMMIT")
      log2("commited changes")
    },
    async finally() {
      log2("cisconnecting from database")
      await db.end()
      await open.tunnel?.close()
    }
  }
})

async function collection_handler(db: PgClient, col: Collection, first: any): Promise<CollectionHandler> {

  const table = col.name
  const temp_table_name = `${table.replace('.', '__')}_temp`
  const columns = Object.keys(first)
  var types = columns.map(c => typeof first[c] === 'number' ? 'real'
  : first[c] instanceof Date ? 'timestamptz'
  : first[c] instanceof Buffer ? 'blob'
  : 'text')
  // console.log(chunk.collection, types)

  if (opts.drop) {
    await db.query(/* sql */`DROP TABLE IF EXISTS ${table}`)
  }

  // Create the table if it didn't exist
  if (opts.auto_create) {
    await db.query(/* sql */`
      CREATE TABLE IF NOT EXISTS ${table} (
        ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
      )
    `)
  }

  if (opts.truncate) {
    log2(`truncating ${table}`)
    await db.query(/* sql */`DELETE FROM ${table}`)
  }

  // Create a temporary table that will receive all the data through pg COPY
  // command. This table will hold plain json objects
  // log2("Creating temp table", temp_table_name)
  await db.query(/* sql */`
    CREATE TEMP TABLE ${temp_table_name} (
      jsondata json
    )
  `)

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

  return {
    async data(data) {
      if (!stream.write('@' + JSON.stringify(data).replace(/@/g, '@@') + '@\n')) {
        drain_lock = new Lock()
        await drain_lock.promise
        drain_lock = null
      }
    },
    async end() {
      // log2("closing COPY pipe")
      stream.end()

      // Figure out if the input name is dotted or not. If not, then use "public" ?
      let schema = opts.schema
      let table_name = table
      if (table.includes("."))
        [schema, table_name] = table.split(".")

      // If we're trying to UPSERT data instead of plain INSERT, figure out the
      // primary key constraint name for this table, and use it.
      // Note: we should be able to specify the constraint manually, maybe not just the primary key ?
      let upsert = ""
      if (opts.upsert) {
        var cst = (await db.query(/* sql */`
          SELECT
            constraint_name
          FROM information_schema.table_constraints
          WHERE table_name = '${table_name}' AND constraint_schema = '${schema}'
            AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE')
          ORDER BY constraint_type
        `))

        upsert = /* sql */ ` ON CONFLICT ON CONSTRAINT "${cst.rows[0].constraint_name}" DO UPDATE SET ${columns.map(c => `"${c}" = EXCLUDED."${c}"`)} `
      }

      // Now we have enough information to actually perform the INSERT statement
      // log2("Inserting data into", table, "from temp table")

      await db.query(/* sql */`
        INSERT INTO ${table}(${columns.map(c => `"${c}"`).join(', ')}) (
          SELECT ${columns.map(c => 'R."' + c + '"').join(', ')}
          FROM ${temp_table_name} T,
            json_populate_record(null::${table}, T.jsondata) R
        )
        ${upsert}
      `)

      // Drop the temporary table
      await db.query(/* sql */`
        DROP TABLE ${temp_table_name}
      `)

      // The following instructions are used to reset sequences if we have have forced
      // id blocks.
      // First, we get all the sequences associated with this table
      const seq_res = await db.query(/* sql */`
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
        await db.query(/* sql */`
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
