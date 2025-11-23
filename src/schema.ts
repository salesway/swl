// These are the base type names that duckdb will return in its describe command
export type DDBType =
  | "BIGINT"
  | "BIT"
  | "BOOLEAN"
  | "BLOB"
  | `DECIMAL(${number}, ${number})`
  | "DATE"
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
  columns: { column_name: string; column_type: Type }[]
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
    if (token.trim() !== "") {
      result.push(token)
    }
    re.lastIndex = last = match.index + token.length
  }

  if (last !== type_name.length) {
    throw new Error(
      `Invalid type: ${type_name} / parser stopped at ${type_name.slice(last)}`
    )
  }

  return result
}

class Parser {
  constructor(public tokens: string[]) {}
  pos = 0

  expect(token: string): void {
    if (this.tokens[this.pos].toLowerCase() !== token.toLowerCase()) {
      throw new Error(`Expected ${token} but got ${this.tokens[this.pos]}`)
    }
    this.pos++
  }

  consume(token: string): boolean
  consume(token: RegExp): RegExpMatchArray
  consume(token: string | RegExp): RegExpMatchArray | boolean {
    const tk = this.tokens[this.pos]
    if (tk == null) {
      return false
    }

    if (typeof token === "string") {
      if (tk.toLowerCase() !== token.toLowerCase()) {
        return false
      }
      this.pos++
      return true
    }

    let match = token.exec(tk)
    if (match === null) {
      return false
    }
    this.pos++
    return match
  }

  next(): string | undefined {
    const tk = this.tokens[this.pos++]
    return tk
  }
}

function _parse(p: Parser): Type {
  let res: Type
  if (p.consume("struct")) {
    res = {
      type: "STRUCT",
      columns: [],
    }
    p.expect("(")
    while (!p.consume(")")) {
      const name = p.next()
      if (name === undefined) {
        throw new Error("Expected column name")
      }
      // console.error("sending at", p.tokens[p.pos])
      const type = _parse(p)
      res.columns.push({ column_name: name, column_type: type })
      p.consume(",")
    }
  } else if (p.consume("union")) {
    res = {
      type: "UNION",
      members: [],
    }
    p.expect("(")
    while (!p.consume(")")) {
      const member = _parse(p)
      res.members.push(member)
      p.consume(",")
    }
    p.expect(")")
  } else if (p.consume("map")) {
    res = {
      type: "MAP",
    } as Map
    p.expect("(")
    res.key = _parse(p)
    p.expect(",")
    res.value = _parse(p)
    p.expect(")")
  } else {
    res = p.next() as Type
  }

  if (p.consume("[]")) {
    return {
      type: "LIST",
      value: res,
    }
  }

  let match: RegExpMatchArray | null = null
  if ((match = p.consume(/^\[\d+\]$/))) {
    return {
      type: "ARRAY",
      value: res,
      length: parseInt(match[0].slice(1, -1)),
    }
  }

  return res
}

export type Column = {
  column_name: string
  column_type: Type
  not_null: boolean
}

/** */
export function parse_duckdb_describe_type(type: string): Type {
  const tk = lex_duckdb_describe_type(type)
  const typ = _parse(new Parser(tk))
  return typ
}

export function duckdb_type_to_string(type: Type): string {
  if (typeof type === "string") {
    return type
  }
  if (type.type === "ARRAY") {
    return duckdb_type_to_string(type.value) + "[" + type.length + "]"
  }
  if (type.type === "LIST") {
    return duckdb_type_to_string(type.value) + "[]"
  }
  if (type.type === "MAP") {
    return (
      "MAP(" +
      duckdb_type_to_string(type.key) +
      ", " +
      duckdb_type_to_string(type.value) +
      ")"
    )
  }
  if (type.type === "STRUCT") {
    return (
      "STRUCT(" +
      type.columns
        .map((c) => c.column_name + " " + duckdb_type_to_string(c.column_type))
        .join(", ") +
      ")"
    )
  }
  if (type.type === "UNION") {
    return (
      "UNION(" +
      type.members.map((m) => duckdb_type_to_string(m)).join(", ") +
      ")"
    )
  }
  throw new Error(`Unknown type: ${JSON.stringify(type)}`)
}
