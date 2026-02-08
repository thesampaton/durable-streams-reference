#!/usr/bin/env node

// Generates test JWTs signed with the local RSA test key.
//
// Usage:
//   node generate-token.mjs                   # valid token (1h expiry)
//   node generate-token.mjs --expired          # expired token
//   node generate-token.mjs --wrong-issuer     # wrong iss claim
//   node generate-token.mjs --no-audience      # missing aud claim
//   node generate-token.mjs --sub user123      # custom sub claim

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { SignJWT, importPKCS8 } from "jose";

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);

function hasFlag(name) {
  return args.includes(`--${name}`);
}

function getFlagValue(name) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const pemContent = await readFile(join(__dirname, "fixtures", "test-key.pem"), "utf8");
const privateKey = await importPKCS8(pemContent, "RS256");

const now = Math.floor(Date.now() / 1000);
const sub = getFlagValue("sub") || "test-user";
const iss = hasFlag("wrong-issuer") ? "wrong-issuer" : "durable-streams-test";
const exp = hasFlag("expired") ? now - 3600 : now + 3600;

const builder = new SignJWT({ sub })
  .setProtectedHeader({ alg: "RS256", kid: "test-key-1" })
  .setIssuer(iss)
  .setIssuedAt(now)
  .setExpirationTime(exp);

if (!hasFlag("no-audience")) {
  builder.setAudience("durable-streams");
}

const token = await builder.sign(privateKey);
process.stdout.write(token);
