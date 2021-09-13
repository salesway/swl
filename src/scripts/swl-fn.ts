import { emit, sink } from "../index"
import * as vm from "vm"
import * as ramda from "ramda"
import { optparser, arg } from "../optparse"

const opts_src = optparser(
  arg("fn").required()
)

let opts = opts_src.parse()

let glob: any = {...ramda, namenorm(v: string) {
  v = typeof v !== 'string' ? '' + v : v
  return v.toLowerCase().normalize("NFD").replace(/['\u0300-\u036f]/g, "").trim().replace(/-/g, " ")
}}
vm.createContext(glob)

let _fn = opts.fn.includes("return") ? opts.fn : `return (${opts.fn})`
const fn = vm.compileFunction(_fn, ["$"], { parsingContext: glob })

sink(function () {
  return {
    collection(col) {
      emit.collection(col.name)
      return {
        async data(data) {
          let res = await fn(data)
          emit.data(res)
        },
        end() {

        }
      }
    },
    end() {

    }
  }
})
