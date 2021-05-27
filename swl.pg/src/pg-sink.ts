#!/usr/bin/env -S node --enable-source-maps

import { CollectionHandler, log, optparser, sink, uri_maybe_open_tunnel, Lock, Collection } from 'swl'

import { Client as PgClient, types } from 'pg'
import { from as copy_from } from 'pg-copy-streams'

let col_parser = optparser()
  .arg("name")
  .flag("truncate", {short: "t", long: "truncate"})
  .flag("drop", {short: "d", long: "drop"})
  .flag("upsert", {short: "u", long: "upsert"})

let opts_parser = optparser()
  .arg("uri")
  .flag("disable_triggers", { short: "t", long: "disable-triggers", help: "Disable triggers before loading data" })
  .flag("notice", { short: "n", long: "notice", help: "Display NOTICE statements" })
  .flag("notify", { short: "y", long: "notify", help: "Display LISTEN/NOTIFY requests" })
  .flag("truncate", {short: "t", long: "truncate", help: "Always truncate tables before inserting data"})
  .flag("drop", {short: "d", long: "drop"})
  .flag("upsert", {short: "u", long: "upsert"})
  .option("schema", { short: "s", long: "schema" })
  .flag("passthrough", {short: "p", long: "passthrough", help: "Forward all data to the next pipe"})
  .flag("verbose", {short: "v", long: "verbose", help: "Display statements run on the sink"})
  .sub("collections", col_parser)
  .post(opts => {
    for (let c of opts.collections) {
      if (opts.truncate) c.truncate = true
      if (opts.drop) c.drop = true
      if (opts.upsert) c.upsert = true
    }

    if (!opts.uri) throw new Error("pg-sink expects a URI")
  })

let opts = opts_parser.parse()

// Date type, don't remember if this is essential or not.
types.setTypeParser(1082, val => {
  // var d = new Date(val)
  return val
})


sink(async () => {
  verbose_log("Connecting to database...")
  let open = await uri_maybe_open_tunnel(opts.uri)
  let uri = open.uri.startsWith("postgres://") ? open.uri : `postgres://${open.uri}`

  let db = new PgClient(uri)

  return {
    passthrough: opts.passthrough,

    // Setup the database and some global options, such as displaying notices
    async init() {
      await db.connect()
      verbose_log("Connected")

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
      verbose_log("Commited changes")
    },
    async finally() {
      verbose_log("Disconnecting from database")
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
  await db.query(/* sql */`
    CREATE TABLE IF NOT EXISTS ${table} (
      ${columns.map((c, i) => `"${c}" ${types[i]}`).join(', ')}
    )
  `)

  // Create a temporary table that will receive all the data through pg COPY
  // command. This table will hold plain json objects
  // verbose_log("Creating temp table", temp_table_name)
  await db.query(/* sql */`
    CREATE TEMP TABLE ${temp_table_name} (
      jsondata json
    )
  `)

  // this.columns_str = columns.map(c => `"${c}"`).join(', ')

  if (opts.truncate) {
    verbose_log(`truncating ${table}`)
    await db.query(/* sql */`DELETE FROM ${table}`)
  }

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
      // verbose_log("closing COPY pipe")
      stream.end()

      // Figure out if the input name is dotted or not. If not, then use "public" ?
      let schema = opts.schema ?? "public"
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
      // verbose_log("Inserting data into", table, "from temp table")

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
        verbose_log(`Resetting sequence ${seq.seq}`)
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

function verbose_log(...a: any[]) {
  if (opts.verbose) log(...a)
}