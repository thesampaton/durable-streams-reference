# Writing a client

You can interact with the DS server using any HTTP client. This page shows patterns for the `@durable-streams/client` SDK, plain `fetch`, and SSE consumption.

## Using the client SDK

The [`@durable-streams/client`](https://www.npmjs.com/package/@durable-streams/client) SDK provides typed methods for stream operations:

```javascript
import { DurableStream, stream } from "@durable-streams/client";

const baseUrl = "http://localhost:4437";

// Create a stream
await DurableStream.create({
  url: `${baseUrl}/v1/stream/my-stream`,
  contentType: "application/json",
});

// Append data
await DurableStream.append({
  url: `${baseUrl}/v1/stream/my-stream`,
  body: JSON.stringify([{ event: "click" }]),
  contentType: "application/json",
});

// Read data
const res = await stream({
  url: `${baseUrl}/v1/stream/my-stream`,
  live: false,
  json: true,
});
const data = await res.json();
const offset = res.offset; // save for resumption
```

### SSE with the client SDK

When using SSE with JSON streams, pass `json: true` explicitly. The SSE transport uses `Content-Type: text/event-stream`, which masks the stream's actual content type:

```javascript
const res = await stream({
  url: `${baseUrl}/v1/stream/my-stream`,
  live: "sse",
  json: true, // required: SSE transport masks the stream's content type
});

for await (const item of res.subscribeJson()) {
  console.log("Received:", item);
}
```

See [ecosystem interop CI-001](../project/gaps.md) for why this hint is necessary.

## Using plain fetch

### Create

```javascript
await fetch(`${baseUrl}/v1/stream/my-stream`, {
  method: "PUT",
  headers: { "Content-Type": "text/plain" },
});
```

### Append

```javascript
await fetch(`${baseUrl}/v1/stream/my-stream`, {
  method: "POST",
  headers: { "Content-Type": "text/plain" },
  body: "hello world",
});
```

### Read with offset resumption

```javascript
// First read
const res = await fetch(`${baseUrl}/v1/stream/my-stream?offset=-1`);
const body = await res.text();
const nextOffset = res.headers.get("Stream-Next-Offset");

// Resume later (only new messages)
const res2 = await fetch(`${baseUrl}/v1/stream/my-stream?offset=${nextOffset}`);
```

## SSE consumption pattern

### Node.js (manual parsing)

```javascript
const res = await fetch(`${baseUrl}/v1/stream/my-stream?live=sse&offset=-1`);
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buffer = "";

while (true) {
  const { done, value } = await reader.read();
  if (done) break;

  buffer += decoder.decode(value, { stream: true });
  const lines = buffer.split("\n");
  buffer = lines.pop(); // keep incomplete line

  let currentEventType = null;
  for (const line of lines) {
    if (line.startsWith("event:")) {
      currentEventType = line.slice(6).trim();
    } else if (line.startsWith("data:") && currentEventType === "data") {
      const payload = line.slice(5);
      console.log("Data:", payload);
    } else if (line.startsWith("data:") && currentEventType === "control") {
      const control = JSON.parse(line.slice(5));
      if (control.streamClosed) {
        console.log("Stream closed");
        reader.cancel();
      }
    } else if (line.startsWith("id:")) {
      const offset = line.slice(3).trim();
      // Save for resumption on reconnect
    }
  }
}
```

### JSON streams over SSE

For JSON-mode streams, each `data:` line wraps the message in a JSON array. Unwrap it:

```javascript
if (currentEventType === "data") {
  const parsed = JSON.parse(line.slice(5));
  const items = Array.isArray(parsed) ? parsed : [parsed];
  for (const item of items) {
    console.log("Item:", item);
  }
}
```

## With authentication

When the DS server is behind an Envoy auth proxy, include a Bearer token:

```javascript
const headers = { Authorization: `Bearer ${token}` };

await fetch(`${baseUrl}/v1/stream/my-stream`, {
  method: "PUT",
  headers: { ...headers, "Content-Type": "text/plain" },
});
```

## Producer idempotency

For exactly-once writes, include producer headers:

```javascript
let seq = 0;

async function append(data) {
  const res = await fetch(`${baseUrl}/v1/stream/my-stream`, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain",
      "Producer-ID": "my-client-tab-1",
      "Producer-Epoch": "0",
      "Producer-Seq": String(seq),
    },
    body: data,
  });

  if (res.status === 200) {
    seq++; // accepted
  } else if (res.status === 204) {
    seq++; // duplicate, already persisted
  }
  // 409: sequence gap, 403: epoch fenced
}
```

See [Producers](../protocol/producers.md) for the full protocol.
