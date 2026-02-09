// Heartbeat producer: alternates between PG->DS and DS->PG directions every
// PRODUCER_INTERVAL_MS (default 5000ms) to exercise both sync paths.
//
// Odd ticks:  INSERT into Postgres `items` table
//             -> Electric WAL -> sync-service -> `pg-items` DS stream
// Even ticks: POST session event to `session-events` DS stream
//             -> sync-service SSE consumer -> `session_events` PG table

import pg from "pg";

const DS_SERVER_URL = process.env.DS_SERVER_URL || "http://server:4437";
const POSTGRES_URL =
  process.env.POSTGRES_URL ||
  "postgresql://postgres:password@postgres:5432/durable_streams";
const INTERVAL_MS = parseInt(process.env.PRODUCER_INTERVAL_MS || "5000", 10);

const DS_BASE = `${DS_SERVER_URL}/v1/stream`;

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

// Register a stream in __registry__ using the durable-streams state protocol.
// The test-ui reads __registry__ to populate its sidebar.
async function registerStream(name, contentType) {
  const event = {
    type: "stream",
    key: name,
    value: {
      path: name,
      contentType,
      createdAt: Date.now(),
    },
    headers: {
      operation: "insert",
      txid: crypto.randomUUID(),
    },
  };
  const res = await fetch(`${DS_BASE}/__registry__`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(event),
  });
  if (res.ok) {
    console.log(`Registered "${name}" in __registry__`);
  } else {
    console.error(
      `Failed to register "${name}" in __registry__: ${res.status}`,
    );
  }
}

async function main() {
  console.log("Heartbeat producer starting...");
  console.log(`  DS Server URL: ${DS_SERVER_URL}`);
  console.log(`  Postgres URL: ${POSTGRES_URL.replace(/:[^:@]*@/, ":***@")}`);
  console.log(`  Interval: ${INTERVAL_MS}ms`);

  // Wait for DS server
  await waitForHealth(`${DS_SERVER_URL}/healthz`, "DS server");

  // Connect to Postgres
  const pgClient = new pg.Client({ connectionString: POSTGRES_URL });
  await pgClient.connect();
  console.log("Postgres connected");

  // Ensure streams exist (including __registry__ for the test-ui)
  await ensureStream("__registry__", "application/json");
  await ensureStream("pg-items", "application/json");
  await ensureStream("session-events", "application/json");

  // Register streams in __registry__ using the state protocol format
  // so the test-ui can discover them in the sidebar
  await registerStream("pg-items", "application/json");
  await registerStream("session-events", "application/json");

  console.log("Heartbeat producer ready, starting tick loop...");

  let tick = 0;
  setInterval(async () => {
    tick++;
    const timestamp = new Date().toISOString();

    try {
      if (tick % 2 === 1) {
        // Odd tick: PG -> DS direction
        // INSERT into items table, which Electric picks up via WAL
        // and sync-service forwards to the pg-items DS stream
        const title = `heartbeat-${tick}`;
        const body = JSON.stringify({
          direction: "PG->DS",
          tick,
          timestamp,
        });
        await pgClient.query(
          "INSERT INTO items (title, body) VALUES ($1, $2)",
          [title, body],
        );
        console.log(
          `[tick ${tick}] PG->DS: INSERT items (title=${title}) at ${timestamp}`,
        );
      } else {
        // Even tick: DS -> PG direction
        // POST session event to DS stream, which sync-service picks up
        // via SSE and INSERTs into the session_events PG table
        const event = {
          key: `heartbeat:${tick}`,
          type: "heartbeat",
          operation: "set",
          value: { direction: "DS->PG", tick, timestamp },
        };
        const res = await fetch(`${DS_BASE}/session-events`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(event),
        });
        if (res.ok) {
          console.log(
            `[tick ${tick}] DS->PG: POST session-events (key=${event.key}) at ${timestamp}`,
          );
        } else {
          console.error(
            `[tick ${tick}] DS->PG: POST failed ${res.status} ${await res.text()}`,
          );
        }
      }
    } catch (err) {
      console.error(`[tick ${tick}] Error: ${err.message}`);
    }
  }, INTERVAL_MS);
}

main().catch((err) => {
  console.error("Heartbeat producer fatal error:", err);
  process.exit(1);
});
