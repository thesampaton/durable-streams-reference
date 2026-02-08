// End-to-end integration tests for the authenticated durable streams stack.
//
// These tests run against the Docker stack (server + Envoy JWT proxy) and
// validate that the full deployment works: auth enforcement, stream CRUD,
// offset resumption, and live SSE subscriptions.
//
// Requires: docker-compose up (server + envoy running on :8080)

import { describe, it, expect, beforeAll } from "vitest";
import { DurableStream, stream } from "@durable-streams/client";
import { generateToken } from "./test-utils.mjs";

const BASE_URL = process.env.E2E_BASE_URL || "http://localhost:8080";
const STREAM_BASE = `${BASE_URL}/v1/stream`;

// Generate a unique stream name per test run to avoid collisions
function streamName(suffix) {
  const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  return `e2e-${suffix}-${id}`;
}

let validToken;

beforeAll(async () => {
  validToken = await generateToken();
});

function authHeaders() {
  return { Authorization: `Bearer ${validToken}` };
}

describe("e2e integration tests", () => {
  // 1. Health check bypass — GET /healthz without JWT -> 200
  it("health check bypasses auth", async () => {
    const res = await fetch(`${BASE_URL}/healthz`);
    expect(res.status).toBe(200);
  });

  // 2. Unauthenticated rejection — PUT without JWT -> 401
  it("rejects unauthenticated requests", async () => {
    const name = streamName("noauth");
    const res = await fetch(`${STREAM_BASE}/${name}`, { method: "PUT" });
    expect(res.status).toBe(401);
  });

  // 3. Expired token rejection — GET with expired JWT -> 401
  it("rejects expired tokens", async () => {
    const expiredToken = await generateToken({ expired: true });
    const name = streamName("expired");
    const res = await fetch(`${STREAM_BASE}/${name}`, {
      headers: { Authorization: `Bearer ${expiredToken}` },
    });
    expect(res.status).toBe(401);
  });

  // 4. Create stream — DurableStream.create() with valid JWT -> stream exists
  it("creates a stream with valid JWT", async () => {
    const name = streamName("create");
    const url = `${STREAM_BASE}/${name}`;

    const ds = await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/octet-stream",
    });

    expect(ds).toBeDefined();

    // Verify stream exists via HEAD
    const head = await DurableStream.head({ url, headers: authHeaders() });
    expect(head.exists).toBe(true);
  });

  // 5. Append and read — append data, read back -> data matches
  it("appends and reads data", async () => {
    const name = streamName("readwrite");
    const url = `${STREAM_BASE}/${name}`;

    const ds = await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "text/plain",
    });

    await ds.append("hello world");

    const res = await stream({
      url,
      headers: authHeaders(),
      live: false,
    });

    const text = await res.text();
    expect(text).toContain("hello world");
  });

  // 6. Offset resumption — read from saved offset -> resumes, no replay
  it("resumes from a saved offset", async () => {
    const name = streamName("resume");
    const url = `${STREAM_BASE}/${name}`;

    const ds = await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "text/plain",
    });

    await ds.append("message-1");
    await ds.append("message-2");

    // Read all and save offset
    const firstRead = await stream({
      url,
      headers: authHeaders(),
      live: false,
    });
    const allText = await firstRead.text();
    expect(allText).toContain("message-1");
    expect(allText).toContain("message-2");
    const savedOffset = firstRead.offset;

    // Append more data
    await ds.append("message-3");

    // Resume from saved offset — should only get message-3
    const resumed = await stream({
      url,
      headers: authHeaders(),
      offset: savedOffset,
      live: false,
    });
    const resumedText = await resumed.text();
    expect(resumedText).toContain("message-3");
    expect(resumedText).not.toContain("message-1");
    expect(resumedText).not.toContain("message-2");
  });

  // 7. SSE live subscription — subscribe, append new data -> arrives live
  it("receives live data via SSE", async () => {
    const name = streamName("sse");
    const url = `${STREAM_BASE}/${name}`;

    const ds = await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/json",
    });

    // Start a live subscription (json hint needed because SSE transport
    // returns Content-Type: text/event-stream, masking the stream's real type)
    const res = await stream({
      url,
      headers: authHeaders(),
      live: "sse",
      json: true,
    });

    const received = [];
    let resolve;
    const liveDataArrived = new Promise((r) => {
      resolve = r;
    });

    const unsubscribe = res.subscribeJson(async (batch) => {
      for (const item of batch.items) {
        received.push(item);
      }
      if (received.length >= 2) {
        resolve();
      }
    });

    // Append data after subscribing
    await ds.append(JSON.stringify({ seq: 1 }));
    await ds.append(JSON.stringify({ seq: 2 }));

    // Wait for both messages (with timeout)
    await Promise.race([
      liveDataArrived,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("SSE timeout")), 10_000),
      ),
    ]);

    unsubscribe();
    res.cancel();

    expect(received).toHaveLength(2);
    expect(received[0]).toEqual({ seq: 1 });
    expect(received[1]).toEqual({ seq: 2 });
  }, 15_000);

  // 8. Delete stream — delete via client -> stream gone (404 on re-read)
  it("deletes a stream", async () => {
    const name = streamName("delete");
    const url = `${STREAM_BASE}/${name}`;

    await DurableStream.create({
      url,
      headers: authHeaders(),
      contentType: "application/octet-stream",
    });

    // Verify it exists
    const headBefore = await DurableStream.head({ url, headers: authHeaders() });
    expect(headBefore.exists).toBe(true);

    // Delete it
    await DurableStream.delete({ url, headers: authHeaders() });

    // Verify it's gone
    const res = await fetch(url, { headers: authHeaders() });
    expect(res.status).toBe(404);
  });
});
