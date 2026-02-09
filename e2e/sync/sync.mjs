// Bidirectional sync service: Electric SQL (PG -> DS) + SSE consumer (DS -> PG).
//
// Direction 1 (PG -> Stream):
//   Subscribes to Electric Shape API for the `items` table.
//   On each change batch, POSTs as JSON array to the `pg-items` DS stream.
//
// Direction 2 (Stream -> PG):
//   Subscribes to the `session-events` DS stream via SSE.
//   On each data event, parses JSON and INSERTs into the `session_events` table.

import { ShapeStream } from "@electric-sql/client";
import pg from "pg";

const ELECTRIC_URL = process.env.ELECTRIC_URL || "http://electric:3000";
const DS_SERVER_URL = process.env.DS_SERVER_URL || "http://server:4437";
const POSTGRES_URL =
  process.env.POSTGRES_URL ||
  "postgresql://postgres:password@postgres:5432/durable_streams";

const DS_BASE = `${DS_SERVER_URL}/v1/stream`;
const PG_ITEMS_STREAM = "pg-items";
const SESSION_EVENTS_STREAM = "session-events";

// Retry helper: wait for a service to become healthy
async function waitForHealth(url, label, maxRetries = 30) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await fetch(url);
      if (res.ok) {
        console.log(`${label} healthy`);
        return;
      }
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`${label} not healthy after ${maxRetries}s`);
}

// Ensure a DS stream exists (PUT is idempotent)
async function ensureStream(name, contentType) {
  const url = `${DS_BASE}/${name}`;
  const res = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": contentType },
  });
  if (res.status === 201 || res.status === 200) {
    console.log(`Stream "${name}" ready (${res.status})`);
  } else {
    const body = await res.text();
    throw new Error(
      `Failed to create stream "${name}": ${res.status} ${body}`,
    );
  }
}

// Direction 1: PG -> Stream via Electric Shape API
async function startPgToStream() {
  await ensureStream(PG_ITEMS_STREAM, "application/json");

  const stream = new ShapeStream({
    url: `${ELECTRIC_URL}/v1/shape`,
    params: { table: "items" },
  });

  let changeCount = 0;

  stream.subscribe(async (messages) => {
    // Filter out control messages (no operation header)
    const changes = messages.filter(
      (msg) => msg.headers && msg.headers.operation,
    );
    if (changes.length === 0) return;

    const payload = changes.map((msg) => ({
      key: msg.key,
      operation: msg.headers.operation,
      value: msg.value,
    }));

    try {
      const res = await fetch(`${DS_BASE}/${PG_ITEMS_STREAM}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        changeCount += changes.length;
        console.log(
          `Forwarded ${changes.length} items changes to DS (total: ${changeCount})`,
        );
      } else {
        console.error(`DS POST failed: ${res.status} ${await res.text()}`);
      }
    } catch (err) {
      console.error("Error forwarding to DS:", err.message);
    }
  });

  console.log("PG -> Stream subscription established");
}

// Direction 2: Stream -> PG via SSE
// Establishes the SSE connection (awaits the HTTP response), then runs the
// read loop in the background. Returns once the connection is open so main()
// can proceed to log readiness.
async function startStreamToPg(pgClient) {
  await ensureStream(SESSION_EVENTS_STREAM, "application/json");

  const sseUrl = `${DS_BASE}/${SESSION_EVENTS_STREAM}`;
  let lastOffset = null;
  let eventCount = 0;

  // Read loop: processes SSE events from an open response. Returns when
  // the connection closes; caller is responsible for reconnection.
  async function readLoop(res) {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        console.log("SSE connection closed, reconnecting...");
        return;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop(); // keep incomplete line

      let currentEventType = null;
      for (const line of lines) {
        if (line.startsWith("event:")) {
          currentEventType = line.slice(6).trim();
        } else if (line.startsWith("data:") && currentEventType === "data") {
          const data = line.slice(5);
          try {
            const parsed = JSON.parse(data);
            // SSE data events for JSON-mode streams wrap each message
            // in an array: [{"key":"..."}]. Unwrap to get individual items.
            const items = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of items) {
              await persistEvent(pgClient, item);
              eventCount++;
            }
            console.log(
              `Persisted ${items.length} session event(s) to PG (total: ${eventCount})`,
            );
          } catch (e) {
            console.error(
              "SSE data parse/persist error:",
              e.message,
              "raw:",
              data.slice(0, 200),
            );
          }
        } else if (line.startsWith("id:")) {
          const id = line.slice(3).trim();
          if (id) lastOffset = id;
        }
      }
    }
  }

  // Reconnection loop (runs in background after initial connect)
  async function reconnect() {
    while (true) {
      await new Promise((r) => setTimeout(r, 1000));
      const offset = lastOffset || "-1";
      const url = `${sseUrl}?live=sse&offset=${offset}`;
      try {
        const res = await fetch(url);
        if (!res.ok) {
          console.error(`SSE reconnect failed: ${res.status}`);
          continue;
        }
        console.log("Stream -> PG SSE reconnected");
        await readLoop(res);
      } catch (err) {
        console.error("SSE reconnect error:", err.message);
      }
    }
  }

  // Initial connect: use live=sse with offset=-1 to get all existing data
  // plus live updates. Await the HTTP response to ensure we're subscribed
  // before tests start writing events.
  const initialUrl = `${sseUrl}?live=sse&offset=-1`;
  const res = await fetch(initialUrl);
  if (!res.ok) {
    throw new Error(`SSE initial connect failed: ${res.status}`);
  }
  console.log("Stream -> PG SSE connection established");

  // Start read loop in background (don't await — it runs until close)
  readLoop(res)
    .then(() => reconnect())
    .catch((err) => {
      console.error("SSE read loop error:", err.message);
      reconnect().catch(() => {});
    });
}

async function persistEvent(pgClient, event) {
  const streamName = SESSION_EVENTS_STREAM;
  const eventKey = event.key || null;
  const eventType = event.type || null;
  const operation = event.operation || null;
  const payload = JSON.stringify(event);

  try {
    await pgClient.query(
      `INSERT INTO session_events (stream_name, event_key, event_type, operation, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [streamName, eventKey, eventType, operation, payload],
    );
  } catch (err) {
    console.error("PG insert error:", err.message);
  }
}

async function main() {
  console.log("Sync service starting...");
  console.log(`  Electric URL: ${ELECTRIC_URL}`);
  console.log(`  DS Server URL: ${DS_SERVER_URL}`);
  console.log(`  Postgres URL: ${POSTGRES_URL.replace(/:[^:@]*@/, ":***@")}`);

  // Wait for upstream services
  await waitForHealth(`${DS_SERVER_URL}/healthz`, "DS server");
  await waitForHealth(`${ELECTRIC_URL}/v1/health`, "Electric");

  // Connect to Postgres
  const pgClient = new pg.Client({ connectionString: POSTGRES_URL });
  await pgClient.connect();
  console.log("Postgres connected");

  // Start both directions
  await startPgToStream();
  await startStreamToPg(pgClient);

  console.log("Sync service ready");
}

main().catch((err) => {
  console.error("Sync service fatal error:", err);
  process.exit(1);
});
