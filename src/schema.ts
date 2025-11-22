// These are the base type names that duckdb will return in its describe command
export type DDBType =
  | "BIGINT"
  | "BIT"
  | "BOOLEAN"
  | "BLOB"
  | `DECIMAL(${number}, ${number})`
  | "DOUBLE"
  | "FLOAT"
  | "HUGEINT"
  | "INTEGER"
  | "INTERVAL"
  | "JSON"
  | "SMALLINT"
  | "TIME"
  | "TIMESTAMP WITH TIME ZONE"
  | "TIMESTAMP"
  | "TINYINT"
  | "UBIGINT"
  | "UHUGEINT"
  | "UINTEGER"
  | "USMALLINT"
  | "UTINYINT"
  | "UUID"
  | "VARCHAR"

export type Map = {
  type: "MAP"
  key: Type
  value: Type
}
export type Array = {
  type: "ARRAY"
  value: Type
  length: number
}
export type List = { type: "LIST"; value: Type }
export type Struct = {
  type: "STRUCT"
  columns: { columnName: string; columnType: Type }[]
}
export type Union = { type: "UNION"; members: Type[] }

export type Type = DDBType | Map | Array | List | Struct | Union

export type DescribeResult = {
  column_name: string
  column_type: string
  null: boolean
  key: string
  default: string
  extra: string
}

const re =
  /"[^"]*"|\s+|\[(\d+)?\]|\(|\)|,|bigint|bit|boolean|blob|date|decimal\(\d+,\s*\d+\)|double|float|hugeint|integer|interval|json|smallint|time(stamp(\s+with\s+time\s+zone)?)?|tinyint|ubigint|uhugeint|uinteger|usmallint|utinyint|uuid|varchar|struct|union|[\p{L}_][\p{L}\p{N}_$]*/iuy

/** tokenize the type name */
function lex_duckdb_describe_type(type_name: string): string[] {
  type_name = type_name.trim()
  const result: string[] = []
  let match: RegExpExecArray | null

  let last = 0
  re.lastIndex = last
  while ((match = re.exec(type_name)) !== null) {
    let token = match[0]
    result.push(token)
    re.lastIndex = last = match.index + token.length
  }

  if (last !== type_name.length) {
    throw new Error(
      `Invalid type: ${type_name} / parser stopped at ${type_name.slice(last)}`
    )
  }

  return result
}

/** */
export function parse_duckdb_describe_type(type: string): Type {
  console.error(lex_duckdb_describe_type(type))
}
