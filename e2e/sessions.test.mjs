// Sessions + Database Sync integration tests.
//
// Validates the full production architecture:
// - Durable Sessions pattern (collaborative AI chat)
// - Bidirectional Postgres sync (PG -> DS via Electric, DS -> PG via consumer)
//
// Requires: docker-compose --profile sync up (full stack including PG, Electric, sync-service)

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { DurableStream, stream } from "@durable-streams/client";
import { generateToken } from "./test-utils.mjs";
import pg from "pg";

const BASE_URL = process.env.E2E_BASE_URL || "http://localhost:8080";
const STREAM_BASE = `${BASE_URL}/v1/stream`;
const PG_URL =
  process.env.PG_URL ||
  "postgresql://postgres:password@localhost:54321/durable_streams";

function streamName(suffix) {
  const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return `session-${suffix}-${id}`;
}

let validToken;

beforeAll(async () => {
  validToken = await generateToken();
});

function authHeaders() {
  return { Authorization: `Bearer ${validToken}` };
}

// Helper: poll until a condition is met or timeout
async function pollUntil(fn, { timeout = 30_000, interval = 500, label = "condition" } = {}) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result;
    await new Promise((r) => setTimeout(r, interval));
  }
  throw new Error(`pollUntil timed out after ${timeout}ms waiting for: ${label}`);
}

// ── Sessions pattern tests ──────────────────────────────────────────────────

describe("sessions pattern", () => {
  // 1. Session lifecycle: create + presence
  it("creates a session and writes a presence event", async () => {
    const name = streamName("lifecycle");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    // Write a STATE-PROTOCOL presence event
    const presenceEvent = {
      key: "user:test-user",
      type: "presence",
      operation: "set",
      value: { status: "online", lastSeen: new Date().toISOString() },
    };

    const appendRes = await fetch(url, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify([presenceEvent]),
    });
    // Non-producer appends always return 204 (no producer sequence tracking)
    expect(appendRes.status).toBe(204);

    // Read back and verify
    const res = await stream({
      url,
      headers: authHeaders(),
      live: false,
      json: true,
    });
    const data = await res.json();
    // JSON-mode read returns an array of all stored messages
    expect(data).toEqual([presenceEvent]);
  });

  // 2. Multi-producer chat: user message + AI response
  it("supports multi-producer chat with ordering", async () => {
    const name = streamName("chat");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    // User message
    const userMsg = {
      key: "msg:1",
      type: "chunk",
      role: "user",
      content: "Hello, AI!",
    };
    await fetch(url, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
        "Producer-ID": "user-producer",
        "Producer-Epoch": "1",
        "Producer-Seq": "0",
      },
      body: JSON.stringify([userMsg]),
    });

    // AI response chunks
    const aiChunk1 = {
      key: "msg:2",
      type: "chunk",
      role: "assistant",
      content: "Hello! ",
    };
    const aiChunk2 = {
      key: "msg:2",
      type: "chunk",
      role: "assistant",
      content: "How can I help?",
    };
    await fetch(url, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
        "Producer-ID": "ai-producer",
        "Producer-Epoch": "1",
        "Producer-Seq": "0",
      },
      body: JSON.stringify([aiChunk1]),
    });
    await fetch(url, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
        "Producer-ID": "ai-producer",
        "Producer-Epoch": "1",
        "Producer-Seq": "1",
      },
      body: JSON.stringify([aiChunk2]),
    });

    // Read all back — should be in order
    const res = await stream({
      url,
      headers: authHeaders(),
      live: false,
      json: true,
    });

    const items = [];
    const unsubscribe = res.subscribeJson((batch) => {
      for (const item of batch.items) {
        items.push(item);
      }
    });

    // Wait for all items
    await pollUntil(() => items.length >= 3 || null, { timeout: 5000 });
    unsubscribe();
    res.cancel();

    expect(items).toHaveLength(3);
    expect(items[0].role).toBe("user");
    expect(items[1].role).toBe("assistant");
    expect(items[2].role).toBe("assistant");
    expect(items[0].content).toBe("Hello, AI!");
    expect(items[2].content).toBe("How can I help?");
  }, 15_000);

  // 3. SSE live subscription
  it("delivers messages live via SSE", async () => {
    const name = streamName("sse-live");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    // Start SSE subscription
    const res = await stream({
      url,
      headers: authHeaders(),
      live: "sse",
      json: true,
    });

    const received = [];
    let resolve;
    const allArrived = new Promise((r) => {
      resolve = r;
    });

    const unsubscribe = res.subscribeJson((batch) => {
      for (const item of batch.items) {
        received.push(item);
      }
      if (received.length >= 2) resolve();
    });

    // Write while subscribed
    await fetch(url, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([{ type: "user-msg", content: "ping" }]),
    });
    await fetch(url, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([{ type: "ai-response", content: "pong" }]),
    });

    await Promise.race([
      allArrived,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("SSE timeout")), 10_000),
      ),
    ]);

    unsubscribe();
    res.cancel();

    expect(received).toHaveLength(2);
    expect(received[0].content).toBe("ping");
    expect(received[1].content).toBe("pong");
  }, 15_000);

  // 4. Session recovery (offset resumption)
  it("recovers a session from a saved offset", async () => {
    const name = streamName("recovery");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    // Write initial messages
    await fetch(url, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([{ seq: 1, content: "before-disconnect" }]),
    });
    await fetch(url, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([{ seq: 2, content: "also-before" }]),
    });

    // Read all and save offset
    const firstRead = await stream({
      url,
      headers: authHeaders(),
      live: false,
      json: true,
    });
    await firstRead.text(); // consume
    const savedOffset = firstRead.offset;
    expect(savedOffset).toBeDefined();

    // Write more messages (simulating activity after disconnect)
    await fetch(url, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([{ seq: 3, content: "after-reconnect" }]),
    });

    // Resume from saved offset — only new messages
    const resumed = await stream({
      url,
      headers: authHeaders(),
      offset: savedOffset,
      live: false,
      json: true,
    });
    const resumedText = await resumed.text();
    expect(resumedText).toContain("after-reconnect");
    expect(resumedText).not.toContain("before-disconnect");
    expect(resumedText).not.toContain("also-before");
  });

  // 5. Producer idempotency (exactly-once writes)
  it("deduplicates retried producer writes", async () => {
    const name = streamName("idempotent");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    const producerHeaders = {
      ...authHeaders(),
      "Content-Type": "application/json",
      "Producer-ID": "idempotent-producer",
      "Producer-Epoch": "1",
      "Producer-Seq": "0",
    };

    // First write
    const first = await fetch(url, {
      method: "POST",
      headers: producerHeaders,
      body: JSON.stringify([{ msg: "first" }]),
    });
    expect(first.status).toBe(200);

    // Retry same write — should be 204 (duplicate)
    const retry = await fetch(url, {
      method: "POST",
      headers: producerHeaders,
      body: JSON.stringify([{ msg: "first" }]),
    });
    expect(retry.status).toBe(204);

    // Next seq — should be accepted
    const next = await fetch(url, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
        "Producer-ID": "idempotent-producer",
        "Producer-Epoch": "1",
        "Producer-Seq": "1",
      },
      body: JSON.stringify([{ msg: "second" }]),
    });
    expect(next.status).toBe(200);
  });
});

// ── Database sync tests ─────────────────────────────────────────────────────

describe("database sync", () => {
  let pgClient;

  beforeAll(async () => {
    pgClient = new pg.Client({ connectionString: PG_URL });
    await pgClient.connect();
  });

  afterAll(async () => {
    if (pgClient) await pgClient.end();
  });

  // 6. PG -> Stream: Postgres insert appears in DS
  it("syncs Postgres inserts to DS stream", async () => {
    const title = `test-item-${Date.now()}`;

    // Insert into Postgres
    await pgClient.query("INSERT INTO items (title, body) VALUES ($1, $2)", [
      title,
      "test body",
    ]);

    // Poll the pg-items stream until the data appears
    const pgItemsUrl = `${STREAM_BASE}/pg-items`;

    const found = await pollUntil(
      async () => {
        try {
          const res = await stream({
            url: pgItemsUrl,
            headers: authHeaders(),
            live: false,
            json: true,
          });
          const text = await res.text();
          if (text.includes(title)) return true;
        } catch {
          // stream might not exist yet if sync service hasn't started
        }
        return null;
      },
      { timeout: 30_000, interval: 500, label: "PG item in DS stream" },
    );

    expect(found).toBe(true);
  }, 35_000);

  // 7. Stream -> PG: DS session event persisted to Postgres
  it("persists DS session events to Postgres", async () => {
    const eventKey = `event-${Date.now()}`;

    // Write a session event to DS via Envoy
    const sessionEventsUrl = `${STREAM_BASE}/session-events`;
    const event = {
      key: eventKey,
      type: "user-action",
      operation: "click",
      data: { button: "submit" },
    };

    const appendRes = await fetch(sessionEventsUrl, {
      method: "POST",
      headers: { ...authHeaders(), "Content-Type": "application/json" },
      body: JSON.stringify([event]),
    });
    // Non-producer appends always return 204
    expect(appendRes.status).toBe(204);

    // Poll Postgres until the event appears
    const found = await pollUntil(
      async () => {
        const result = await pgClient.query(
          "SELECT * FROM session_events WHERE event_key = $1",
          [eventKey],
        );
        if (result.rows.length > 0) return result.rows[0];
        return null;
      },
      { timeout: 30_000, interval: 500, label: "session event in PG" },
    );

    expect(found.event_key).toBe(eventKey);
    expect(found.event_type).toBe("user-action");
    expect(found.operation).toBe("click");
    const payload = found.payload;
    expect(payload.key).toBe(eventKey);
    expect(payload.data.button).toBe("submit");
  }, 35_000);

  // 8. Round trip: PG -> DS -> client reads
  it("delivers Postgres inserts to DS stream (round trip)", async () => {
    const title = `roundtrip-item-${Date.now()}`;
    const pgItemsUrl = `${STREAM_BASE}/pg-items`;

    // Insert into Postgres — should arrive via Electric -> sync-service -> DS
    await pgClient.query("INSERT INTO items (title, body) VALUES ($1, $2)", [
      title,
      "round-trip test",
    ]);

    // Poll the DS stream until our item appears
    const found = await pollUntil(
      async () => {
        try {
          const res = await stream({
            url: pgItemsUrl,
            headers: authHeaders(),
            live: false,
            json: true,
          });
          const text = await res.text();
          if (text.includes(title)) return true;
        } catch {
          // stream read error, retry
        }
        return null;
      },
      { timeout: 30_000, interval: 500, label: "PG round-trip item in DS" },
    );

    expect(found).toBe(true);
  }, 35_000);
});
