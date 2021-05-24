const re_date = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?:\d{2}(?:\.\d{3}Z?)))?$/
const re_number = /^\d+(\.\d+)?$/
const re_boolean = /^true|false$/i

export function uncoerce(value: any) {
  if (value && (value[0] === '{' || value[0] === '[')) {
    try {
      return JSON.parse(value)
    } catch (e) {
      return value
    }
  }

  if (typeof value === 'string') {
    var trimmed = value.trim().toLowerCase()

    if (trimmed.match(re_date)) {
      return new Date(trimmed)
    }

    if (trimmed.match(re_boolean)) {
      return trimmed.toLowerCase() === 'true'
    }

    if (trimmed.match(re_number)) {
      return parseFloat(trimmed)
    }

    if (trimmed === 'null')
      return null
  }

  return value
}


export function coerce(value: any) {
  const typ = typeof value
  if (value === null || typ === 'string' || typ === 'number' || value instanceof Buffer) {
    return value
  }
  if (typ === 'boolean')
    return value ? 'true' : 'false'
  if (value === undefined)
    return null

  if (value instanceof Date) {
    return (new Date(value.valueOf() - (value.getTimezoneOffset() * 60000))).toISOString()
  } //if (Array.isArray(value))
    //return value.join(', ')

  return JSON.stringify(value)
}
