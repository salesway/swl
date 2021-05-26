
export interface FlagOpts {
  short: string
  long?: string
  help?: string
}

export class OptionParser<T = {}> {
  private handlers: ((inst: T, args: string[], pos: number) => number)[] = []
  private builders: ((inst: T) => void)[] = []

  prebuild(): T {
    let res = {} as T
    for (let b of this.builders) b(res)
    return res
  }

  flag<K extends string>(key: K, opts: FlagOpts): OptionParser<T & {[key in K]: boolean}> {
    let short = '-' + opts.short
    let long = opts?.long ? '--' + opts?.long : ''

    this.handlers.push(function flag(inst: any, args: string[], pos: number) {
      let arg = args[pos]
      if (arg === short || arg === long) {
        // yes !
        inst[key] = true
        return pos + 1
      }
      return pos
    })
    return this as any
  }

  option<K extends string, U>(key: K, opts: FlagOpts, transform: (a: string) => U): OptionParser<T & {[key in K]: U}>
  option<K extends string>(key: K, opts: FlagOpts): OptionParser<T & {[key in K]: string}>
  option<K extends string>(key: K, opts: FlagOpts, transform?: (a: string) => any): OptionParser<T & {[key in K]: any}> {
    let short = '-' + opts.short
    let long = opts?.long ? '--' + opts?.long : ''
    this.handlers.push(function option(inst: any, args, pos) {
      let arg = args[pos]
      if (arg !== short && arg !== long) return pos
      if (pos >= args.length - 1) throw new Error(`option ${short} expects an argument`)
      let value = args[pos + 1]

      inst[key] = transform?.(value) ?? value
      return pos + 2
    })

    return this as any
  }

  arg<K extends string>(key: K): OptionParser<T & {[key in K]: string}> {
    let found = Symbol('found-' + key)
    this.handlers.push(function arg(inst: any, args, pos) {
      if (inst[found]) return pos
      let arg = args[pos]
      if (arg[0] === '-') return pos
      inst[key] = arg
      Object.defineProperty(inst, found, { enumerable: false, value: true })
      return pos + 1
    })
    return this as any
  }

  sub<K extends string, V>(key: K, kls: OptionParser<V>): OptionParser<T & {[key in K]: V[]}> {
    this.builders.push((i: any) => i[key] = [])
    this.handlers.push(function group(inst: any, args, pos) {
      inst[key] = []
      let subres = kls.prebuild()
      let res = kls.doParse(subres, args, pos)
      inst[key].push(subres)
      return res
    })
    return this as any
  }

  _post?: (t: T) => any
  post<U>(fn: (t: T) => U): U extends void | Promise<void> ? OptionParser<T> : OptionParser<U> {
    this._post = fn
    return this as any
  }

  private doParse(inst: T, args: string[], pos: number) {
    let l = args.length
    scanargs: while (pos < l) {
      for (let h of this.handlers) {
        let res = h(inst, args, pos)
        if (typeof res === 'number' && res > pos) {
          pos = res
          continue scanargs
        }
      }
      break
    }
    return pos
  }

  parse(args: string[] = process.argv.slice(2)): T {
    let res = this.prebuild()
    let pos = this.doParse(res, args, 0)

    if (pos !== args.length) {
      throw new Error(`leftovers: ${args.slice(pos).join(' ')}`)
    }

    return this._post?.(res) ?? res
  }
}

export function optparser() { return new OptionParser() }
