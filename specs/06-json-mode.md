# JSON Mode

## Overview

Streams with `Content-Type: application/json` operate in JSON mode, which provides
special semantics for batching and reading JSON values. JSON mode enables efficient
batch appends while maintaining the abstraction that each message is an independent
JSON value.

## Append Semantics

### Single JSON Value

When appending a single JSON value to a JSON stream, it is stored as-is:

```
POST /v1/stream/events
Content-Type: application/json

{"event": "click", "x": 100}
```

This stores one message containing the JSON object.

### JSON Array Flattening

When appending a JSON array, each element is stored as a separate message:

```
POST /v1/stream/events
Content-Type: application/json

[
  {"event": "click", "x": 100},
  {"event": "scroll", "y": 50}
]
```

This stores **two messages**, one for each array element. The array wrapper is
removed (flattened). This enables efficient batching on the client side while
maintaining per-message semantics on the server.

### Empty Array Rejection

Empty arrays are not allowed in JSON mode:

```
POST /v1/stream/events
Content-Type: application/json

[]
```

Returns `400 Bad Request` with error message indicating empty arrays are not permitted.

This prevents ambiguity: an empty array could mean "append zero messages" or
"append one message containing an empty array". By rejecting empty arrays,
the semantics remain clear.

### Nested Arrays

Arrays nested within a top-level object or within array elements are preserved:

```
POST /v1/stream/data
Content-Type: application/json

[
  {"tags": ["a", "b"]},
  {"items": [1, 2, 3]}
]
```

This stores two messages:
1. `{"tags": ["a", "b"]}`
2. `{"items": [1, 2, 3]}`

Only the **top-level** array is flattened. Nested arrays are part of the value.

### Invalid JSON

Invalid JSON returns `400 Bad Request`:

```
POST /v1/stream/events
Content-Type: application/json

{invalid json}
```

JSON validation occurs before storage. Malformed JSON is rejected immediately.

## Read Semantics

### Array Wrapping

When reading from a JSON stream, **all messages are wrapped in a JSON array**:

```
GET /v1/stream/events?offset=-1

[
  {"event": "click", "x": 100},
  {"event": "scroll", "y": 50}
]
```

Even if only one message is read, it is wrapped in an array:

```
GET /v1/stream/events?offset=-1

[
  {"event": "click", "x": 100}
]
```

This provides consistent output format regardless of the number of messages.

### Empty Stream

Reading from an empty JSON stream returns an empty array:

```
GET /v1/stream/events?offset=-1

[]
```

This is distinct from the empty array rejection on append: reading produces a
valid JSON array, but appending an empty array is rejected to avoid semantic
ambiguity.

### Mixed Batching

Clients can append a mix of single values and arrays:

```
POST 1: {"a": 1}           → stores 1 message
POST 2: [{"b": 2}, {"c": 3}] → stores 2 messages
POST 3: {"d": 4}           → stores 1 message

GET returns: [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}]
```

The read output is the concatenation of all stored messages, wrapped in an array.

## Non-JSON Content Types

JSON mode is **only activated** when `Content-Type: application/json` is used.

Streams with other content types (e.g., `text/plain`, `application/octet-stream`)
do **not** perform array flattening or wrapping. They concatenate raw bytes.

Attempting to append JSON to a non-JSON stream does not trigger JSON mode:

```
PUT /v1/stream/data
Content-Type: text/plain

POST /v1/stream/data
Content-Type: text/plain

[{"a": 1}, {"b": 2}]
```

This stores the literal string `[{"a": 1}, {"b": 2}]` as one message. No flattening.

## Content-Type Consistency

As with all streams, the content type is immutable after creation. You cannot
mix JSON and non-JSON appends to the same stream:

```
PUT /v1/stream/data
Content-Type: application/json

POST /v1/stream/data
Content-Type: text/plain
Body: "text"
```

Returns `409 Conflict` (content type mismatch).

## Charset Parameter

The `charset` parameter is ignored for JSON mode (as with all content types):

```
Content-Type: application/json; charset=utf-8
```

Is treated identically to:

```
Content-Type: application/json
```

JSON is always UTF-8 by specification (RFC 8259).

## Testing Requirements

Implementations must validate:

1. Single JSON value appends as one message
2. JSON array flattens into multiple messages
3. Empty arrays return 400
4. Invalid JSON returns 400
5. Nested arrays are preserved (only top-level flattened)
6. GET wraps all messages in an array
7. GET from empty stream returns `[]`
8. Mixed batching (single values and arrays) concatenates correctly
9. Non-JSON streams do not perform flattening/wrapping
