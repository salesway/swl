#!/usr/bin/env -S node --enable-source-maps

import { unflatten } from "flat"
import { sink, emit } from "../index"

sink(function () {
  return {
    collection(col) {
      emit.collection(col.name)
      return {
        data(data) {
          emit.data(unflatten(data))
        },
        end() { /* do nothing */ }
      }
    },
    end() { /* do nothing */ }
  }
})