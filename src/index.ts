import * as v8 from "v8"
import * as fs from "fs"
import * as tty from "tty"
import * as path from "path"

import { ChunkType, Chunk, Data, ErrorChunk, Message, Collection, chunk_is_message, chunk_is_collection, chunk_is_data, chunk_is_error } from "./types"
import { coll, debug, num, c } from "./debug"

export * from "./types"
export * from "./debug"

export const col_sink = c.hsl(1, 40, 40)
export const col_src = c.hsl(170, 40, 40)
export const col_alias = c.rgb(111, 111, 111)
export const col_table = c.rgb(14, 130, 130)

export function log(...a: any[]) {
  console.error(self_name, ...a)
}


const out = fs.openSync("/dev/stdout", "w")

export namespace emit {

  // The whole emit facilities use the synchronized write API.
  // Why not process.stdout ? Simply because it is a tad slower in most
  // situations, due to the asynchronous dispatching of data, and
  // because it would force the implementer to await on each emit to check
  // for "drain" events when the backpressure is too high.
  //
  // There are situations where it might be slower, but so far I have not
  // met them.

  const header = new Uint8Array(5)
  const len_buf = new Uint32Array(header.buffer, 0, 1)

  let L = 4 * 1024
  let offset = 0
  const output_buf = new Uint8Array(L)

  // Flush to stdout
  function flush() {
    let view = new Uint8Array(output_buf.buffer, 0, offset)
    fs.writeFileSync(out, view)
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
  process.on("beforeExit", _ => {
    if (offset > 0)
      flush()
  })

  export function write_packet(type: ChunkType, buf: Uint8Array) {
    header[4] = type
    len_buf[0] = buf.length

    write(header)
    write(buf)
  }

  export function write_chunk(type: ChunkType, packet: Chunk) {
    write_packet(type, v8.serialize(packet))
  }

  export const chunk = tty.isatty(out) ? debug : write_chunk

  let _count = 0
  let _current: string | null = null
  export function data(data: Data) {
    _count++
    chunk(ChunkType.Data, data)
  }

  export function collection(name: string) {
    if (_current != null) log1(coll(_current), "emitted", num(_count), "lines")
    _current = name
    _count = 0
    chunk(ChunkType.Collection, { name })
  }

  process.on("beforeExit", _ => {
    if (_current != null) log1(coll(_current), "emitted", num(_count), "lines")
  })

  export function error(err: ErrorChunk) {
    let stack = {stack: ""}
    Error.captureStackTrace(stack, error)
    err.origin = self_name
    err.stack = stack.stack
    chunk(ChunkType.Error, err)
  }
}


export function report_error(err: ErrorChunk) {
  if (tty.isatty(out)) {
    log("error: ", err)
  } else {
    emit.error(err)
  }
}


export function packet_reader() {
  // let fd = fs.openSync("/dev/stdin", "rs")
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
      // log("!!", offset, length, res, l)
      if (res === 0) {
        if (offset === 0) return 0
        throw new Error(`unexpected EOF (want: ${l} but had 0, try: ${length}, offset: ${offset})`) // EOF !
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
      // log("read", length, rd, total)
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
  init?(): Promise<void> | void
  message?(message: Message): Promise<void> | void
  error?(error: ErrorChunk): Promise<void> | void
  end(): Promise<void> | void
  finally?(): Promise<void> | void
  passthrough?: boolean
}

export async function sink(_handler: Handler | (() => Promise<Handler> | Handler)) {

  let handler = typeof _handler === "function" ? await _handler() : _handler
  let reader = packet_reader()

  let collection_handler: CollectionHandler | null = null
  let collection_name = ""
  let collection: Collection | null = null
  let _count = 0

  if (tty.isatty(0)) throw new Error(`a sink needs an input`)
  let read: null | ReturnType<ReturnType<typeof packet_reader>["next"]>

  try {
    await handler.init?.()
  } catch (e) {
    log(e)
    throw e
  }

  while ((read = reader.next())) {
    let type = read.type

    if (handler.passthrough) {
      emit.write_packet(type, read.view)
    }

    let chunk: Chunk = v8.deserialize(read.view)

    // Now that the next packet is read, dispatch it to the correct method
    if (chunk_is_collection(type, chunk)) {
      if (collection_handler) {
        await end_collection()
      }
      collection = chunk
      collection_name = chunk.name
    } else if (chunk_is_data(type, chunk)) {
      _count++
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
      await handler.finally?.()
      return
    }

    // console.log(packet)
  }

  await end_collection()

  async function end_collection() {
    if (!collection_handler) return
    await collection_handler.end()
    collection_handler = null
    log1(coll(collection_name), "received", num(_count), "lines")
    collection = null
    _count = 0
  }

  await handler.end()
  await handler.finally?.()
}


// The current executable name, used in target: when passing commands and messages.
export let self_name: string = path.basename(process.argv[1]).replace(".js", "").replace("swl-", "")
if (self_name.includes("-src"))
  self_name = col_src(self_name.replace("-src", " » "))
if (self_name.includes("sink"))
  self_name = col_sink(self_name.replace("-sink", " « "))


/**
 * Try to find a forward pattern in an URI and create the ssh tunnel if
 * found.
 *
 * @param uri: the uri to search the pattern in
 * @returns a modified URI with the forwarded port on localhost
 */
export async function uri_maybe_open_tunnel(uri: string) {
  const gp = (require("get-port") as typeof import("get-port"))
  const tunnel = require("tunnel-ssh") as typeof import("tunnel-ssh")
  const conf = require("ssh-config")
  const promisify = (require("util") as typeof import("util")).promisify

  var local_port = await gp()
  var re_tunnel = /([^@:\/]+):(\d+)@@(?:([^@:]+)(?::([^@]+))?@)?([^:/]+)(?::([^\/]+))?/

  var match = re_tunnel.exec(uri)

  // If there is no forward to create, just return the uri as-is
  if (!match) return { uri }

  const [remote_host, remote_port, user, password, host, port] = match.slice(1)

  var config: any = {
    host, port: port,
    dstHost: remote_host, dstPort: remote_port,
    localPort: local_port, localHost: "127.0.0.1"
  }

  if (user) config.username = user
  if (password) config.password = password

  try {
    var _conf = conf.parse(fs.readFileSync(`${process.env.HOME}/.ssh/config`, "utf-8"))
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
  let res = await promisify(tunnel)(config)
  return { uri: uri.replace(match[0], `127.0.0.1:${local_port}`), tunnel: res }
}


export function emit_upstream(): boolean {

  if (tty.isatty(0)) {
    return true
  }

  let should_forward = !tty.isatty(out)
  let reader = packet_reader()
  let read: null | ReturnType<ReturnType<typeof packet_reader>["next"]>
  while ((read = reader.next())) {
    let type = read.type
    let view = read.view

    if (should_forward) {
      fs.writeSync(1, reader.header)
      fs.writeSync(1, view)
    } else {
      let obj = v8.deserialize(view)
      debug(type, obj)
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


////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

export { optparser, FlagOpts, OptionParser } from "./optparse"
import { optparser } from "./optparse"

process.on("uncaughtException", err => {
  emit.error({message: err.message})
  log(err)
  process.exit(1)
})

export class Lock<T> {

  promise: Promise<T>
  _accept!: (t: T) => void
  _reject!: (e: any) => void
  constructor() {

    this.promise = new Promise((accept, reject) => {
      this._accept = accept
      this._reject = reject
    })
  }

  resolve(t: T) {
    this._accept(t)
  }

  reject(e: any) {
    this._reject(e)
  }
}

export let log1 = (...a: any[]) => { }
export let log2 = (...a: any[]) => { }
export let log3 = (...a: any[]) => { }

export const default_opts = optparser()
  .option("alias", {short: "a", long: "alias", default: "", post: inst => {
    self_name = self_name + col_alias("(" + inst["alias"] + ")")
  }})
  .flag("verbose", {short: "v", long: "verbose", post: inst => {
    let verb = inst["verbose"]
    if (verb >= 1) log1 = log
    if (verb >= 2) log2 = log
    if (verb >= 3) log3 = log
  }})

export const default_col_src_opts = optparser()
  .arg("name")

export const default_col_sql_src_opts = default_col_src_opts.clone()
  .option("query", {short: "q", long: "query"})
