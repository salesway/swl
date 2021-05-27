#!/usr/bin/env -S node --enable-source-maps

class AliasMap {
  add(source: string | null, sink: string | null, ...names: string[]): this {
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
  .add("swl-xls-src", "swl-xls-sink", ".xlsx", ".ods", ".xlsb", ".xls")

let prototols = new AliasMap()
  .add("swl-pg-src", "swl-pg-sink", "postgres://")

let argv = process.argv.slice(2)

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

console.log(commands)