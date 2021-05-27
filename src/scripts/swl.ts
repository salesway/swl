#!/usr/bin/env -S node --enable-source-maps

import { extname, join } from "path"
import { execSync } from "child_process"

class AliasMap {
  map = new Map<string, {source: string | null, sink: string | null}>()

  add(source: string | null, sink: string | null, ...names: string[]): this {
    for (let n of names) { this.map.set(n, {source, sink}) }
    return this
  }
}

let alias = new AliasMap()
  .add(_("pg-src"), _("pg-sink"), "pg")
  .add(_("sqlite-src"), _("sqlite-sink"), "sqlite")
  .add(_("xlsx-src"), _("xlsx-sink"), "xls", "xlsx")
  .add(_("yaml-src"), _("yaml-sink"), "yaml", "yml")
  .add(null, _("flatten"), "flatten")
  .add(null, _("unflatten"), "unflatten")
  .add(null, _("coerce"), "coerce")
  .add(null, _("uncoerce"), "uncoerce")

let file_extensions = new AliasMap()
  .add(_("sqlite-src"), _("sqlite-sink"), ".db", ".sqlite")
  .add(_("xlsx-src"), _("xlsx-sink"), ".xlsx", ".ods", ".xlsb", ".xls", '.xlsm')
  .add(_("yaml-src"), _("yaml-sink"), ".yaml", ".yml")

let protocols = new AliasMap()
  .add(_("pg-src"), _("pg-sink"), "postgres://")
  // add mysql
  // add mssql
  // add oracle


get_commands().then(c => {
  try {
    execSync(c, { stdio: "inherit" })
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

  return [item]
}

async function get_commands() {
  let commands: {command: string[], source: boolean}[] = []
  {
    let command: string[] = []
    commands.push({source: true, command})
    for (let item of process.argv.slice(2)) {
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
    if (b.includes(" "))
      b = `"${b.replace("\"", "\\\"")}"`
    return b
  }).join(" ")
}

function _(name: string) {
  return join(__dirname, `swl-${name}.js`)
}