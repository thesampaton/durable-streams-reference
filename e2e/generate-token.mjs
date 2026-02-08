#!/usr/bin/env node

// Generates test JWTs signed with the local RSA test key.
//
// Usage:
//   node generate-token.mjs                   # valid token (1h expiry)
//   node generate-token.mjs --expired          # expired token
//   node generate-token.mjs --wrong-issuer     # wrong iss claim
//   node generate-token.mjs --no-audience      # missing aud claim
//   node generate-token.mjs --sub user123      # custom sub claim

import { generateToken } from "./test-utils.mjs";

const args = process.argv.slice(2);

function hasFlag(name) {
  return args.includes(`--${name}`);
}

function getFlagValue(name) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const token = await generateToken({
  sub: getFlagValue("sub"),
  expired: hasFlag("expired"),
  wrongIssuer: hasFlag("wrong-issuer"),
  noAudience: hasFlag("no-audience"),
});

process.stdout.write(token);
