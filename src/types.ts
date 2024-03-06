
export const enum ChunkType {
  Data = 1,
  Collection = 2,
  Error = 3,
  Message = 4,
}

export type SwlColumnType =
  | "bool"
  | "text"
  | "int"
  | "float"
  | "json"
  | "date" // Always TZ. If TZ was not specified, UTC should be assumed.

export interface ColumnHelper {
  /** The name of the column */
  name: string
  /** Whether the column is nullable */
  nullable: boolean
  /** The internal type */
  type: SwlColumnType
  /** The type name as specified by the database */
  db_type?: string
  /** Mostly the column is null */
}

export interface Collection {
  name: string
  columns?: ColumnHelper[]
}

export interface ErrorChunk {
  origin?: string
  stack?: string
  payload?: any
  message: string
}

export interface Message {
  origin: string
  target: string
  message: string
  type: string
}

export type Data = any // string | number | boolean | Date | null | Data[] | {[name: string]: Data}

export type Chunk =
  | Data
  | Collection
  | ErrorChunk
  | Message


export function chunk_is_data(type: ChunkType, data: any): data is Data { return type === ChunkType.Data }
export function chunk_is_collection(type: ChunkType, data: any): data is Collection { return type === ChunkType.Collection }
export function chunk_is_error(type: ChunkType, data: any): data is ErrorChunk { return type === ChunkType.Error }
export function chunk_is_message(type: ChunkType, data: any): data is Message { return type === ChunkType.Message }

