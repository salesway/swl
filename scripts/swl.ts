#!/usr/bin/env -S bun run

import { existsSync } from "fs"
import { extname, join } from "path"
import { execSync, } from "child_process"
import { performance } from "perf_hooks"
import { col_num, log } from "../src/index"

import { optparser, flag, } from "../src/optparse"
import ch from "chalk"

class AliasMap {
  map = new Map<string, {source: string | null, sink: string | null}>()

  add(source: string | null, sink: string | null, ...names: string[]): this {
    for (let n of names) { this.map.set(n, {source, sink}) }
    return this
  }
}

let alias = new AliasMap()
  .add(_("pg-src"),     _("pg-sink"),     "pg")
  .add(_("my-src"),     null,             "my", "mysql")
  .add(_("sqlite-src"), _("sqlite-sink"), "sqlite")
  .add(_("duckdb-src"),            _("duckdb-sink"), "duckdb")
  .add(_("xlsx-src"),   _("xlsx-sink"),   "xl", "xls", "xlsx")
  .add(_("yaml-src"),   _("yaml-sink"),   "yaml", "yml")
  .add(_("json-src"),   _("json-sink"),   "json")
  .add(_("csv-src"),    _("csv-sink"),    "csv")
  .add(_("parquet-src"), _("parquet-sink"), "parquet", "pqt")
  .add(null,            _("fn"),          "fn")
  .add(null,            _("flatten"),     "flatten")
  .add(null,            _("unflatten"),   "unflatten")
  .add(null,            _("coerce"),      "coerce")
  .add(null,            _("uncoerce"),    "uncoerce")

let file_extensions = new AliasMap()
  .add(_("csv-src"), _("csv-sink"), ".csv")
  .add(_("parquet-src"), null, ".pqt", ".parquet")
  .add(_("sqlite-src"), _("sqlite-sink"), ".db", ".sqlite")
  .add(_("duckdb-src"), _("duckdb-sink"), ".ddb", ".duckdb")
  .add(_("xlsx-src"),   _("xlsx-sink"),   ".xlsx", ".ods", ".xlsb", ".xls", '.xlsm')
  .add(_("yaml-src"),   _("yaml-sink"),   ".yaml", ".yml")
  .add(_("json-src"),   _("json-sink"),   ".json")

let protocols = new AliasMap()
  .add(_("pg-src"),     _("pg-sink"), "postgres://")
  .add(_("my-src"),     null,         "mysql://")
  // add mysql
  // add mssql
  // add oracle


let start = performance.now()
let pre_flags: string[] = []
let cmd = process.argv.slice(2)
{
  while (cmd[0]?.[0] === "-") {
    pre_flags.push(cmd.shift()!)
  }
}

const opts = optparser(
  flag("--quiet").as("quiet").default(false),
  flag("-v", "--verbose").as("verbose").repeat().map(v => {
    return v.length || 2
  }),
  flag("-h", "--help", "help").as("help"),
).parse(pre_flags)

let verbose: undefined | number = opts.quiet ? undefined : opts.verbose ?? (process.env.SWL_VERBOSE ? parseInt(process.env.SWL_VERBOSE) : 2)

if (opts.help) {
  console.error("Usage: swl ...")
  process.exit(0)
}

get_commands(cmd).then(c => {
  try {
    // console.error(c)
    execSync(c, { stdio: "inherit", env: {
      ...process.env,
      SWL_CHILD: "child",
      SWL_VERBOSE: verbose != null ? "" + verbose : undefined,
    } })
    let end = performance.now()

    if (!process.env.SWL_CHILD)
      log("done in", col_num(Math.round(end - start)) + "ms",)

  } catch (e: any) {
    process.exit(e.status)
  }
})

//////////////////////////////////////////////////////////////////////
//// Utility functions
//////////////////////////////////////////////////////////////////////

/**
 * Figure out which sink or source should be used for the current item.
 * @param item The current argument item
 */
async function figure_out_who(item: string, source: boolean): Promise<string[]> {

  // First figure out if it was named in its alias
  let alv = alias.map.get(item)
  if (alv != null) {
    return ["bun", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink"]
  }

  let ext = extname(item)
  alv = file_extensions.map.get(ext)
  if (alv != null) {
    return ["bun", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink", item]
  }

  let re_protocol = /^[-+a-zA-Z_]+:\/\//
  let match = re_protocol.exec(item)
  if (match != null) {
    alv = protocols.map.get(match[0])
    if (alv != null) {
      return ["bun", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink", item]
    }
  }

  if (existsSync(item)) {
    item = "./" + item
  }

  return [item]
}

async function get_commands(cmd: string[]) {
  // console.log(cmd)
  let commands: {command: string[], source: boolean}[] = []
  {
    let command: string[] = []
    commands.push({source: true, command})
    for (let item of cmd) {
      if (item === "::") {
        // Special case if the command starts with :: to mean we *do* want to begin
        // with a sink, eg. if we're running the command over ssh.
        if (commands.length === 1 && commands[0].command.length === 0)
          commands = []
        command = []
        commands.push({source: false, command})
      } else if (item === "++") {
        command = []
        commands.push({source: true, command})
      } else {
        command.push(item)
      }
    }
  }

  let builder: string[] = []
  let first = true

  function show_aliases(aliases: Map<string, {source: string | null, sink: string | null}>) {
    const lst = [...aliases].sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)
    return lst.map(([key, pro]) => {
      const sigil = pro.source && pro.sink ? ch.magentaBright("⇄")
        : pro.source ? ch.greenBright("←")
        : ch.redBright("→")

      return `  ${sigil} ${key}`
    }).join("\n")
  }

  for (let c of commands) {
    if (!c.command[0]) {
      console.error("error: a command may not be empty\n\n  list of available sources/sinks :\n")
      console.log("handlers:")
      console.log(show_aliases(alias.map) + "\n")
      console.log("extensions:")
      console.log(show_aliases(file_extensions.map) + "\n")
      console.log("protocols:")
      console.log(show_aliases(protocols.map) + "\n")
      process.exit(1)
    }

    if (!first) { builder.push("|") } else { first = false }
    let res = c.command.slice(1)
    let item = await figure_out_who(c.command[0], c.source)
    c.command = [...item, ...res]
    builder = [...builder, ...c.command]
  }

  return builder.map(b => {
    return b === "|" ? b: `"${b.replace(/["]/g, m => "\\" + m)}"`
  }).join(" ")
}

function _(name: string) {
  return join(__dirname, `swl-${name}.ts`)
}