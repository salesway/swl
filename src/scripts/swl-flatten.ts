#!/usr/bin/env -S bun run

import { flatten } from "flat"
import { sink, emit } from "../index"

sink(function () {
  return {
    collection(col) {
      emit.collection(col.name)
      return {
        data(data) {
          emit.data(flatten(data))
        },
        end() { /* do nothing */ }
      }
    },
    end() { /* do nothing */ }
  }
})