import * as v8 from 'v8'
import * as fs from 'fs'
import * as tty from 'tty'
import * as path from 'path'

import 'reflect-metadata'
// import {  } from 'reflect-metadata'

import { ChunkType, Chunk, Data, ErrorChunk, Message, Collection, chunk_is_message, chunk_is_collection, chunk_is_data, chunk_is_error } from './types'
import { debug, file, grey } from './debug'

// import './out'

export function log(...a: any[]) {
  let w = grey(a.map(a => a.toString()).join(' '))
  console.error(`${file(util.self_name)}:`, w)
}

export namespace emit {

  const header = new Uint8Array(5)
  const len_buf = new Uint32Array(header.buffer, 0, 1)

  let L = 4 * 1024
  let offset = 0
  const output_buf = new Uint8Array(L)

  // Flush to stdout
  function flush() {
    let view = new Uint8Array(output_buf.buffer, 0, offset)
    fs.writeFileSync(1, view)
    offset = 0
  }

  /**
   * Write to stdout, buffering manually since writeFileSync will not do it.
   */
  function write(buf: Uint8Array) {
    let len = buf.buffer.byteLength
    let self_offset = 0

    // while the length we have to write is bigger than the available space in the output buffer,
    // buffer stuff out
    while (len > 0) {
      let amount = Math.min(len, L - offset)
      let view = new Uint8Array(buf.buffer, self_offset, amount)
      output_buf.set(view, offset)
      self_offset += amount
      offset += amount
      len -= amount
      if (offset >= L) flush()
    }
  }

  // flush the buffer
  process.on('beforeExit', _ => {
    if (offset > 0)
      flush()
  })

  function write_chunk(type: ChunkType, packet: Chunk) {
    let buf = v8.serialize(packet)
    header[4] = type
    len_buf[0] = buf.length

    write(header)
    write(buf)
  }

  export const chunk = tty.isatty(1) ? debug : write_chunk

  export function data(data: Data) {
    chunk(ChunkType.Data, data)
  }

  export function collection(name: string) {
    chunk(ChunkType.Collection, { name })
  }

  export function error(err: ErrorChunk) {
    let stack = {stack: ''}
    Error.captureStackTrace(stack, error)
    err.origin = util.self_name
    err.stack = stack.stack
    chunk(ChunkType.Error, err)
  }
}


export function report_error(err: ErrorChunk) {
  if (process.stdout.isTTY) {
    log('error: ', err)
  } else {
    emit.error(err)
  }
}


export type Read = {

}

export function packet_reader() {
  // let fd = fs.openSync('/dev/stdin', 'rs')
  let header = new Uint8Array(5)
  let lenreader = new Uint32Array(header.buffer, 0, 1)
  let bufsize = 16 * 1024 // 16 kb by default, can get higher, but it should handle most cases...
  let buf = new Uint8Array(bufsize)
  let rd = -1

  function read_buffer(buffer: Uint8Array, l: number): number {
    let length = l
    let offset = 0
    while (length > 0) {
      let res = fs.readSync(0, buffer, offset, length, null)
      // log('!!', offset, length, res, l)
      if (res === 0) {
        if (offset === 0) return 0
        throw new Error(`unexpected EOF (${l}, ${length}, ${offset})`) // EOF !
      }
      offset += res
      length -= res
    }
    return offset
  }

  return {
    next() {
      if (rd === 0) return null
      rd = read_buffer(header, 5)
      if (rd === 0) return null
      let type = header[4]
      let length = lenreader[0]
      // log(type, length)

      if (length > bufsize) {
        bufsize = Math.max(bufsize * 3 / 2, Math.ceil(length / 1024) * 1024)
        buf = new Uint8Array(bufsize)
      }

      // rd = fs.readSync(fd, buf, 0, length, null)
      // log('read', length, rd, total)
      rd = read_buffer(buf, length)
      if (rd !== length) {
        // THIS IS AN ERROR !!!
        throw new Error(`stdin returned EOF, yet there is data expected (expected ${length})`)
      }
      let view = buf.subarray(0, length)
      return {type, view}
    },
    header: header
  }

}


export namespace sink {

  export interface CollectionHandler {
    // start(coll: Collection, data: Data): Promise<void> | void
    data(data: Data): Promise<void> | void
    end(): Promise<void> | void
  }

  export interface Handler {
    /**
     * Called before collection start
     */
    collection(col: Collection, data: Data): Promise<CollectionHandler> | CollectionHandler
    message?(message: Message): Promise<void> | void
    error?(error: ErrorChunk): Promise<void> | void
    end(): Promise<void> | void
    finally?(): Promise<void> | void
    passthrough?: boolean
  }

  export async function registerHandler(_handler: Handler | (() => Promise<Handler> | Handler)) {

    let handler = typeof _handler === 'function' ? await _handler() : _handler
    let reader = packet_reader()
    let passthrough = handler.passthrough
    ///
    var collection_handler: CollectionHandler | null = null
    var collection: Collection | null = null

    if (tty.isatty(0)) throw new Error(`a sink needs an input`)
    let read: null | ReturnType<ReturnType<typeof packet_reader>['next']>

    while ((read = reader.next())) {
      let type = read.type
      let chunk: Chunk = v8.deserialize(read.view)

      if (passthrough) {
        emit.chunk(type, chunk)
      }

      // Now that the next packet is read, dispatch it to the correct method
      if (chunk_is_collection(type, chunk)) {
        if (collection_handler) {
          await collection_handler.end()
        }
        collection = chunk
      } else if (chunk_is_data(type, chunk)) {
        let data = chunk
        if (collection) {
          collection_handler = await handler.collection(collection, data)
          collection = null
        }
        await collection_handler!.data(chunk)
      } else if (chunk_is_message(type, chunk)) {
        await handler.message?.(chunk)
      } else if (chunk_is_error(type, chunk)) {
        // error will kill the current stream
        // and stop any processing
        if (handler.error) {
          await handler.error(chunk)
        }
        report_error(chunk)
        return
      }

      // console.log(packet)
    }
    await handler.end()
  }

}


export namespace util {

  // The current executable name, used in target: when passing commands and messages.
  export const self_name: string = path.basename(process.argv[1])


  /**
   * Try to find a forward pattern in an URI and create the ssh tunnel if
   * found.
   *
   * @param uri: the uri to search the pattern in
   * @returns a modified URI with the forwarded port on localhost
   */
  export async function uri_maybe_open_tunnel(uri: string): Promise<string> {
    const gp = (require('get-port') as typeof import('get-port'))
    const tunnel = require('tunnel-ssh')
    const conf = require('ssh-config')
    const promisify = (require('util') as typeof import('util')).promisify

    var local_port = await gp()
    var re_tunnel = /([^@:\/]+):(\d+)@@(?:([^@:]+)(?::([^@]+))?@)?([^:/]+)(?::([^\/]+))?/

    var match = re_tunnel.exec(uri)

    // If there is no forward to create, just return the uri as-is
    if (!match) return uri

    const [remote_host, remote_port, user, password, host, port] = match.slice(1)

    var config: any = {
      host, port: port,
      dstHost: remote_host, dstPort: remote_port,
      localPort: local_port, localHost: '127.0.0.1'
    }

    if (user) config.username = user
    if (password) config.password = password

    try {
      var _conf = conf.parse(fs.readFileSync(`${process.env.HOME}/.ssh/config`, 'utf-8'))
      const comp = _conf.compute(host)
      if (comp.HostName) config.host = comp.HostName
      if (comp.User && !config.username) config.username = comp.User
      if (comp.Password && !config.password) config.password = comp.Password
      if (comp.Port && !config.port) config.port = comp.Port

    } catch (e) {
      // console.log(e)
    }

    if (!config.port) config.port = 22

    // Create the tunnel
    await promisify(tunnel)(config)
    return uri.replace(match[0], `127.0.0.1:${local_port}`)
  }

  export function emit_upstream(): boolean {
    // let fd = fs.openSync('/dev/stdin', 'rs')
    if (tty.isatty(0)) {
      return true
    }
    // fs.closeSync(fd)
    // let fd = fs.openSync('/dev/stdout', 'as+')
    // let out = process.stdout
    let should_forward = !tty.isatty(1)
    let reader = packet_reader()
    let read: null | ReturnType<ReturnType<typeof packet_reader>['next']>
    while ((read = reader.next())) {
      let type = read.type
      let view = read.view

      if (should_forward) {
        fs.writeSync(1, reader.header)
        fs.writeSync(1, view)
      }

      if (type === ChunkType.Error) {
        return false
      }
    }
    return true
  }

  /**
   * A very simple function to have the source forward its input and then run the source itself.
   *
   * @param fn The function to execute once stdin has been forwarded
   */
  export function source<T>(fn: () => T) {
    if (emit_upstream())
      return fn()
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////


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

  param<K extends string>(key: K, opts: FlagOpts): OptionParser<T & {[key in K]: string}> {
    let short = '-' + opts.short
    let long = opts?.long ? '--' + opts?.long : ''
    this.handlers.push(function option(inst: any, args, pos) {
      let arg = args[pos]
      if (arg !== short && arg !== long) return pos
      if (pos >= args.length - 1) throw new Error(`option ${short} expects an argument`)
      inst[key] = args[pos + 1]
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
