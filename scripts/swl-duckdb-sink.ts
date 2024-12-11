
import { default_opts, optparser, sink } from ".."
import { arg, flag, oneof } from "../src/optparse"
import { duckdb_sink } from "./duckdb-sink-common"

let col_opts = optparser(
  flag("-t", "--truncate").as("truncate"),
  flag("-d", "--drop").as("drop"),
  flag("-u", "--upsert").as("upsert")
)

let col_parser = optparser(
  arg("name").required(),
  col_opts,
)

let opts_parser = optparser(
  arg("file").required(),
  default_opts,
  col_opts,
  oneof(col_parser).as("collections").repeat(),
)

let opts = opts_parser.parse()

for (let c of opts.collections) {
  if (opts.truncate) c.truncate = true
  if (opts.drop) c.drop = true
  if (opts.upsert) c.upsert = true
}


sink(duckdb_sink(opts.file, {
  truncate: opts.truncate,
  verbose: 2,
  drop: opts.drop,
  upsert: opts.upsert,
}))
