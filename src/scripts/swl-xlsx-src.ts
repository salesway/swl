#!/usr/bin/env -S node --enable-source-maps

import { source, emit } from "../index"
import { arg, oneof, optparser, param } from "../optparse"

import { readFile } from "xlsx"

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


  let wb = readFile(opts.file, { cellHTML: false, cellText: false, cellFormula: false, cellNF: false })

  for (let sname of wb.SheetNames) {
    const s = wb.Sheets[sname]

    if (opts.sources.size > 0 && !opts.sources.has(sname))
      continue

    emit.collection(sname)

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

    // Now that we've got the header, we just go on with the rest of the lines
    var not_found_count = 0
    for (var j = header_line + 1; j <= lines; j++) {
      var obj: {[name: string]: any} = {}
      var found = false
      for (let i = header_column; i < header.length; i++) {
        const cell = s[`${COLS[i]}${j}`]
        if (cell) {
          obj[header[i - header_column]] = cell.v
          found = true
          not_found_count = 0
        } else {
          obj[header[i - header_column]] = null
        }
      }

      if (found)
        emit.data(obj)
      else {
        not_found_count++
        if (not_found_count > 5)
        // More than five empty rows in a row means we're at the end of the document.
          break
      }
    }

  }
})