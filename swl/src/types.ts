
export const enum ChunkType {
  Data = 1,
  Collection = 2,
  Error = 3,
  Message = 4,
}

export interface Collection {
  name: string
}

export interface ErrorChunk {
  origin?: string
  stack?: string
  message: string
}

export interface Message {
  origin: string
  target: string
  message: string
  type: string
}

export type Data = string | number | boolean | Date | null | Data[] | {[name: string]: Data}

export type Chunk =
  | Data
  | Collection
  | ErrorChunk
  | Message


export function chunk_is_data(type: ChunkType, data: any): data is Data { return type === ChunkType.Data }
export function chunk_is_collection(type: ChunkType, data: any): data is Collection { return type === ChunkType.Collection }
export function chunk_is_error(type: ChunkType, data: any): data is ErrorChunk { return type === ChunkType.Error }
export function chunk_is_message(type: ChunkType, data: any): data is Message { return type === ChunkType.Message }

