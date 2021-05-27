#!/usr/bin/env -S node --enable-source-maps

import { sink, optparser, emit } from "../index"
import { readFile } from "xlsx"

let opts = optparser()
  .flag("passthrough", {short: "p", long: "passthrough", help: "Let data flow to the next element"})
  .flag("compression", {short: "c", long: "compression", help: "Enable database compression"})
  .parse()

