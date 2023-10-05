#!/usr/bin/env -S node --enable-source-maps

import { source, emit, log2, col_alias } from "../index"
import { arg, oneof, optparser, param } from "../optparse"
import * as xl from "xlsx"


let collections_opts = optparser(
  arg("name").required(),
  param("-r", "--rename").as("rename").help("Rename this collection"),
)

let opts_ = optparser(
  arg("file").required(),
  oneof(collections_opts).as("collections").repeat(),
)
  .parse()

let opts = {
  ...opts_,
  sources: opts_.collections.reduce((acc, item) => {
    acc.set(item.name, item)
    return acc
  }, new Map<string, typeof opts_["collections"][0]>())
}


if (!opts.file) throw new Error("need a file")


source(function () {

  // Build a list of known columns
  const _l = ['', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
  const COLS: string[] = []

  for (let i = 0; i < _l.length; i++) {
    for (let j = 1; j < _l.length; j++) {
      COLS.push(_l[i] + _l[j])
    }
  }

  const reader = xl.readFile(opts.file)
  let sheet_names = reader.SheetNames.map(s => ({ name: s, rename: s }))

  if (opts.collections.length) {
    sheet_names = opts.collections.map(c => ({ name: c.name, rename: c.rename ?? c.name }))
  }

  for (let c of sheet_names) {
    const s = reader.Sheets[c.name]
    if (!s) {
      emit.error(new Error(`no such sheet "${c.name}"`))
      continue
    }

    const re_range = /^([A-Z]+)(\d+):([A-Z]+)(\d+)$/
    const match = re_range.exec(s['!ref'] as string)
    if (!match) continue

    // Try to figure out if we were given a header position globally
    // or for this specific sheet
    let header_line = 1
    let header_column = 0

    // const re_header = /^([A-Z]+)(\d+)$/
    // const hd = this.options.header || sources && sources![sname]
    // if (typeof hd === 'string') {
    //   var m = re_header.exec(hd)
    //   if (m) {
    //     header_column = COLS.indexOf(m[1])
    //     header_line = parseInt(m[2])
    //   }
    // }
    // We have to figure out the number of lines
    const lines = parseInt(match[4])

    // Then we want to find the header row. By default it should be
    // "A1", or the first non-empty cell we find
    const header: string[] = []
    for (let i = header_column; i < COLS.length; i++) {
      const cell = s[`${COLS[i]}${header_line}`]
      if (!cell || !cell.v)
      break
      header.push(cell.v)
    }

    let emitted_collection = false
    // Now that we've got the header, we just go on with the rest of the lines
    for (var j = header_line + 1; j <= lines; j++) {
      let obj: {[name: string]: any} = {}
      var found = false

      let error: string | null = null
      let error_xl_a1: string | null = null
      for (let i = header_column; i < header.length; i++) {
        const cell = s[`${COLS[i]}${j}`]
        const head = header[i - header_column]
        if (cell) {
          obj[head] = cell.v
          found = found || cell.v != null && cell.v != ""

          if (cell.t === "e") {
            error = head
            error_xl_a1 = `${COLS[i]}${j}`
            obj[head] = cell.w
          }
        } else {
          obj[head] = null
        }
      }

      if (error != null) {
        emit.error({message: `the cell ${error_xl_a1} (${error}) contained an error`, payload: obj })
        return
      }
      if (found) {
        if (!emitted_collection) {
          emit.collection(c.rename)
          emitted_collection = true
        }
        emit.data(obj)
      }
    }

    if (!emitted_collection) {
      log2(`${col_alias(c.rename)} was empty, nothing emitted`)
    }

  }

})