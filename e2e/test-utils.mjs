// Shared test utilities for e2e tests.
//
// Exports generateToken() for programmatic JWT creation.

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { SignJWT, importPKCS8 } from "jose";

const __dirname = dirname(fileURLToPath(import.meta.url));

let _privateKey;

async function getPrivateKey() {
  if (!_privateKey) {
    const pem = await readFile(
      join(__dirname, "fixtures", "test-key.pem"),
      "utf8",
    );
    _privateKey = await importPKCS8(pem, "RS256");
  }
  return _privateKey;
}

/**
 * Generate a signed JWT for testing.
 *
 * @param {object} [options]
 * @param {string} [options.sub]          - Subject claim (default: "test-user")
 * @param {boolean} [options.expired]     - Issue an already-expired token
 * @param {boolean} [options.wrongIssuer] - Use an invalid issuer
 * @param {boolean} [options.noAudience]  - Omit the audience claim
 * @returns {Promise<string>} Signed JWT string
 */
export async function generateToken({
  sub,
  expired,
  wrongIssuer,
  noAudience,
} = {}) {
  const privateKey = await getPrivateKey();
  const now = Math.floor(Date.now() / 1000);

  const builder = new SignJWT({ sub: sub || "test-user" })
    .setProtectedHeader({ alg: "RS256", kid: "test-key-1" })
    .setIssuer(wrongIssuer ? "wrong-issuer" : "durable-streams-test")
    .setIssuedAt(now)
    .setExpirationTime(expired ? now - 3600 : now + 3600);

  if (!noAudience) {
    builder.setAudience("durable-streams");
  }

  return builder.sign(privateKey);
}
