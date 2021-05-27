/*
  Different ways ;

   - flag : counts the number of times a flag was seen. 0 is always the default.
   - option : either looks at '=' or looks at the next argument
   - sub : launches the parsing into another context. May have a "trigger" which can be
        anything (- or not)
   - arg : positional argument. Must come in order. Argument cannot start with a '-'
   - rest : gobbles up whatever it can read, but not option
   - repeat : repeats a sub parser
   - value : sets a value on the resulting object
*/

// TODO: Add a way to specify several short flags ie: -fvi
// TODO: Add a way to *repeat* flags -v -vv -vvv
// TODO: Add a way for flags to optionaly have values or be lists
// TODO: Add validation for some options
// TODO: Add required / optional


export interface FlagOpts<T> {
  short: string
  long?: string
  help?: string
  default?: T
  repeating?: boolean
  post?: (inst: any) => any
  transform?: (s: string) => T
}

export interface Option<K extends string, U> {

  key: K

  builder: () => U

  /**
   * scan the argument at position `pos` in `argv` and reply wether the position was consumed
   * or wether we stayed on the same position but we still consumed the argument
   */
  scan(argv: string[], pos: number): number
}

export type Merge<T, K extends string, U> = T & {[key in K]: U}
export type ProbablyString<U> = unknown extends U ? string : U

function is_simple_flag(item: string) {
  return item[0] === "-" && item[1] !== "-" && item.length > 1
}

export class OptionParser<T = {}> {
  private handlers: ((inst: T, args: string[], pos: number) => number | undefined)[] = []
  private builders: ((inst: any) => void)[] = []
  private post_fns: ((inst: any) => any)[] = []

  private clone<T>() {
    let n = new OptionParser<T>()
    n.builders = this.builders.slice()
    n.handlers = this.handlers.slice() as any
    n.post_fns = this.post_fns.slice()
    return n
  }

  prebuild(): T {
    let res = {} as T
    for (let b of this.builders) b(res)
    return res
  }

  include<U>(other: OptionParser<U>) {
    let n = this.clone<T & U>()
    n.builders = [...this.builders, ...other.builders]
    n.handlers = [...this.handlers, ...other.handlers]
    n.post_fns = [...this.post_fns, ...other.post_fns]
    return n
  }

  flag<K extends string>(key: K, opts: FlagOpts<void>) {
    let n = this.clone<Merge<T, K, number>>()
    let short = opts.short
    let long = opts?.long ? '--' + opts?.long : ''

    if (opts.post)
      n.post_fns.push(opts.post)
    n.builders.push(function (obj) { obj[key] = 0 })
    n.handlers.push(function flag(inst, args, pos) {
      let arg = args[pos]
      if (arg[0] !== "-") return undefined // not a flag, not handled.

      // If it is a simple flag, then count its occurences
      if (is_simple_flag(arg) && arg.includes(short)) {
        for (let i = 0, l = arg.length; i < l; i++) {
          if (arg[i] === short)
            inst[key]++
        }
        return pos // do not move, other flags may trigger
      }

      if (arg === long) {
        inst[key]++
        return pos + 1
      }

      return undefined // not found, not handled.
    })

    return n
  }

  option<K extends string, U, F extends FlagOpts<U>>(key: K, opts: F):
    OptionParser<Merge<T, K,
      undefined extends F["repeating"] ?
        (undefined extends F["default"] ? ProbablyString<U> | undefined : ProbablyString<U>)
      : (undefined extends F["default"] ? ProbablyString<U> | undefined : ProbablyString<U>)[]
    >>

{
    let n = this.clone<any>()

    let short_eql = "-" + opts.short + "="
    let long = opts?.long ? `--${opts.long}=` : null
    let repeating = opts.repeating
    let found = Symbol("found-" + key)

    if (opts.post)
      n.post_fns.push(opts.post)
    let def = opts.default
    if (def) {
      n.builders.push(function (inst) { inst[key] = def })
    }
    if (repeating) {
      n.builders.push(function (inst) { inst[key] = [] })
    }

    n.handlers.push(function option(inst, args, pos) {
      if (!repeating && inst[found]) return undefined
      let arg = args[pos]

      let value: string | null = null
      if (arg.startsWith(short_eql)) {
        value = arg.slice(3)
      } else if (long != null && arg.startsWith(long)) {
        value = arg.slice(long.length)
      } else {
        // not handled
        return undefined
      }

      if (opts.transform)
        value = opts.transform(value) as any
      if (repeating)
        inst[key].push(value)
      else
        inst[key] = value
      return pos + 1
    })

    return this as any
  }

  arg<K extends string>(key: K): OptionParser<T & {[key in K]: string}> {
    let found = Symbol('found-' + key)
    this.handlers.push(function arg(inst: any, args, pos) {
      let arg = args[pos]
      if (inst[found] || arg[0] === "-") return undefined
      inst[key] = arg
      Object.defineProperty(inst, found, { enumerable: false, value: true })
      return pos + 1
    })
    return this as any
  }

  sub<K extends string, V>(key: K, kls: OptionParser<V>): OptionParser<T & {[key in K]: V[]}> {
    this.builders.push(function (i: any) { i[key] = [] })
    this.handlers.push(function group(inst: any, args, pos) {
      if (args[pos][0] === "-") return undefined // can't start on options ?
      inst[key] = []
      let subres = kls.prebuild()
      let res = kls.doParse(subres, args, pos)
      inst[key].push(subres)
      return res
    })
    return this as any
  }

  post<U>(fn: (t: T) => U): U extends void | Promise<void> ? OptionParser<T> : OptionParser<U> {
    this.post_fns.push(fn)
    return this as any
  }

  private doParse(inst: T, args: string[], pos: number) {
    let l = args.length
    scanargs: while (pos < l) {
      let at_least_one = false
      handlers: for (let h of this.handlers) {
        let res = h(inst, args, pos)
        if (typeof res === 'number') {
          if (res > pos) {
            pos = res
            continue scanargs
          } else {
            at_least_one = true
            continue handlers
          }
        }
      }

      if (!at_least_one)
        break
      pos++
    }
    return pos
  }

  parse(args: string[] = process.argv.slice(2)): T {
    let res = this.prebuild()
    let pos = this.doParse(res, args, 0)

    if (pos !== args.length) {
      throw new Error(`leftovers: ${args.slice(pos).join(' ')}`)
    }

    for (let post of this.post_fns) {
      let rpost = post(res)
      if (rpost != null) res = rpost
    }
    return res
  }
}

export function optparser() { return new OptionParser() }
