#!/usr/bin/env -S node --enable-source-maps

import { existsSync } from "fs"
import { extname, join } from "path"
import { execSync } from "child_process"
import { performance } from "perf_hooks"
import { col_num, log } from "../index"

import { optparser, flag, param } from "../optparse"

class AliasMap {
  map = new Map<string, {source: string | null, sink: string | null}>()

  add(source: string | null, sink: string | null, ...names: string[]): this {
    for (let n of names) { this.map.set(n, {source, sink}) }
    return this
  }
}

let alias = new AliasMap()
  .add(_("pg-src"),     _("pg-sink"),     "pg")
  .add(_("sqlite-src"), _("sqlite-sink"), "sqlite")
  .add(_("xlsx-src"),   _("xlsx-sink"),   "xl", "xls", "xlsx")
  .add(_("yaml-src"),   _("yaml-sink"),   "yaml", "yml")
  .add(_("csv-src"),    null,             "csv")
  .add(null,            _("fn"),          "fn")
  .add(null,            _("flatten"),     "flatten")
  .add(null,            _("unflatten"),   "unflatten")
  .add(null,            _("coerce"),      "coerce")
  .add(null,            _("uncoerce"),    "uncoerce")

let file_extensions = new AliasMap()
  .add(_("csv-src"), null, ".csv")
  .add(_("sqlite-src"), _("sqlite-sink"), ".db", ".sqlite")
  .add(_("xlsx-src"),   _("xlsx-sink"),   ".xlsx", ".ods", ".xlsb", ".xls", '.xlsm')
  .add(_("yaml-src"),   _("yaml-sink"),   ".yaml", ".yml")

let protocols = new AliasMap()
  .add(_("pg-src"),     _("pg-sink"), "postgres://")
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

let verbose: undefined | number = process.env.SWL_VERBOSE ? parseInt(process.env.SWL_VERBOSE) : undefined
const opts = optparser(
  flag("-v", "--verbose").as("verbose").repeat().map(v => {
    if (verbose == null)
      verbose = v.length
  }),
  param("-h", "--help").as("help"),
).parse(pre_flags)

if (opts.help) {
  console.error("Usage: swl ...")
  process.exit(0)
}

get_commands(cmd).then(c => {
  try {
    execSync(c, { stdio: "inherit", env: {
      ...process.env,
      SWL_CHILD: "child",
      SWL_VERBOSE: verbose != null ? "" + verbose : undefined,
    } })
    let end = performance.now()

    if (!process.env.SWL_CHILD)
      log("done in", col_num(Math.round(end - start)) + "ms",)

  } catch (e) {
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
    return ["node", "--enable-source-maps", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink"]
  }

  let ext = extname(item)
  alv = file_extensions.map.get(ext)
  if (alv != null) {
    return ["node", "--enable-source-maps", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink", item]
  }

  let re_protocol = /^[-+a-zA-Z_]+:\/\//
  let match = re_protocol.exec(item)
  if (match != null) {
    alv = protocols.map.get(match[0])
    if (alv != null) {
      return ["node", "--enable-source-maps", source? alv.source ?? "non-existent-source" : alv.sink ?? "non-existent-sink", item]
    }
  }

  if (existsSync(item)) {
    item = "./" + item
  }

  return [item]
}

async function get_commands(cmd: string[]) {
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

  for (let c of commands) {
    if (!c.command[0]) throw new Error("a command may not be empty")

    if (!first) { builder.push("|") } else { first = false }
    let res = c.command.slice(1)
    let item = await figure_out_who(c.command[0], c.source)
    c.command = [...item, ...res]
    builder = [...builder, ...c.command]
  }

  return builder.map(b => {
    if (b.includes(" ") || b.includes(";"))
      b = `'${b.replace("\'", "\\\'")}'`
    return b
  }).join(" ")
}

function _(name: string) {
  return join(__dirname, `swl-${name}.js`)
}