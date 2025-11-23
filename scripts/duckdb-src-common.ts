import * as DB from "@duckdb/node-api"
import { Column, DescribeResult, parse_duckdb_describe_type } from "schema"

export async function create_duckdb_helper(
  con: DB.DuckDBConnection,
  query: string
): Promise<Column[]> {
  const desc = await con.runAndReadAll("DESCRIBE " + query)

  return (desc.getRowObjectsJson() as DescribeResult[]).map((desc) => ({
    column_name: desc.column_name,
    column_type: parse_duckdb_describe_type(desc.column_type),
    not_null: desc.null,
  }))
}
