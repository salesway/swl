#!/usr/bin/env -S bun run

import { unflatten } from "flat"
import { sink, emit, optparser } from "../src/index"
import { flag } from "../src/optparse"

const opts_src = optparser(
  // param("-d", "--delimiter").as("delimiter").default(","),
  // param("-q", "--quote").as("quote"),
  flag("-n", "--no-empty").as("noempty"),
  // flag("-N", "--empty-null").as("emptyisnull"),
  // param("-e", "--escape").as("escape"),
  // param("-h", "--headers").as("headers").default(""),
  // param("-c", "--collection").as("collection"),
  // param("-m", "--merge").as("merge").help("Add null columns"),
  // flag("-s", "--simplify-headers").as("simplify_headers"),
  // flag("-n", "--no-empty").as("noempty"),
  // arg("files").required().repeat(),
)

const args = opts_src.parse()


sink(function () {
  return {
    collection(col) {
      emit.collection(col.name)
      return {
        data(data) {

          const unf: any = unflatten(data)

          if (args.noempty) {
            for (let x in unf) {
              let item = unf[x]
              if (item?.constructor === Object) {
                for (let x2 in item) {
                  if (item[x2] == null) {
                    delete item[x2]
                  }
                }
              }
            }
          }

          emit.data(unf)
        },
        end() { /* do nothing */ }
      }
    },
    end() { /* do nothing */ }
  }
})