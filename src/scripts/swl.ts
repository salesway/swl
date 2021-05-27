#!/usr/bin/env -S node --enable-source-maps

class AliasMap {
  map = new Map<string, {source: string | null, sink: string | null}>()

  add(source: string | null, sink: string | null, ...names: string[]): this {
    for (let n of names) { this.map.set(n, {source, sink}) }
    return this
  }
}

let alias = new AliasMap()
  .add("swl-pg-src", "swl-pg-sink", "pg")
  .add("swl-sqlite-src", "swl-sqlite-sink", "sqlite")
  .add("swl-xls-src", "swl-xls-sink", "xls")
  .add(null, "swl-flatten", "flatten")
  .add(null, "swl-unflatten", "unflatten")
  .add(null, "swl-coerce", "coerce")
  .add(null, "swl-uncoerce", "uncoerce")

let file_extensions = new AliasMap()
  .add("swl-sqlite-src", "swl-sqlite-sink", ".db", ".sqlite")
  .add("swl-xls-src", "swl-xls-sink", ".xlsx", ".ods", ".xlsb", ".xls", '.xlsm')

let prototols = new AliasMap()
  .add("swl-pg-src", "swl-pg-sink", "postgres://")


console.log(get_commands())

//////////////////////////////////////////////////////////////////////
//// Utility functions
//////////////////////////////////////////////////////////////////////

function get_commands() {
  let commands: {command: string[], source: boolean}[] = []
  {
    let command: string[] = []
    commands.push({source: true, command})
    for (let item of process.argv.slice(2)) {
      if (item === "::") {
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
  return commands
}
