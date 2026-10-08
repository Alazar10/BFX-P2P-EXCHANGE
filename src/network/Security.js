'use strict';

const crypto = require('node:crypto');

class SecurityGate {
  /**
   * @param {string} clusterSecret - Shared cluster secret or master signing secret
   * @param {number} maxDriftMs - Maximum allowed timestamp drift in ms (default 5000)
   */
  constructor(clusterSecret, maxDriftMs = 5000) {
    if (!clusterSecret || typeof clusterSecret !== 'string') {
      throw new Error('SECURITY_INIT_ERROR: clusterSecret must be a valid non-empty string');
    }
    this.clusterSecret = clusterSecret;
    this.maxDriftMs = maxDriftMs;

    // Outbound client counter: Map<apiKey, bigint>
    this.clientNonces = new Map();

    // Inbound verification watermark: Map<apiKey, bigint>
    this.receivedWatermarks = new Map();
  }

  /**
   * Deterministically derives a unique 64-bit integer userId from an authenticated apiKey.
   * @param {string} apiKey
   * @returns {bigint}
   */
  deriveUserId(apiKey) {
    const hash = crypto.createHash('sha256').update(apiKey).digest();
    return hash.readBigUInt64BE(0);
  }

  _canonicalize(apiKey, timestamp, nonce, data) {
    const serializedData = JSON.stringify(data, Object.keys(data).sort());
    return `${apiKey}:${timestamp}:${nonce}:${serializedData}`;
  }

  /**
   * Signs a payload using HMAC-SHA256.
   * @param {object} data
   * @param {string} apiKey
   * @param {string|number|bigint} customNonce
   * @returns {object}
   */
  sign(data, apiKey = 'trader_default', customNonce = null) {
    const timestamp = Date.now();
    let nonce;

    if (customNonce !== null && customNonce !== undefined) {
      nonce = BigInt(customNonce).toString();
    } else {
      const prev = this.clientNonces.get(apiKey) || 0n;
      const current = BigInt(timestamp);
      nonce = (current > prev ? current : prev + 1n).toString();
      this.clientNonces.set(apiKey, BigInt(nonce));
    }

    const canonical = this._canonicalize(apiKey, timestamp, nonce, data);
    const signature = crypto
      .createHmac('sha256', this.clusterSecret)
      .update(canonical)
      .digest('hex');

    return {
      apiKey,
      timestamp,
      nonce,
      signature,
      data
    };
  }

  /**
   * Validates envelope integrity, timestamp drift, monotonic nonce, and HMAC signature.
   * @param {object} envelope
   * @returns {{ apiKey: string, userId: bigint, data: object }}
   */
  verify(envelope) {
    if (!envelope || typeof envelope !== 'object') {
      throw new Error('SECURITY_REJECT: Malformed envelope');
    }

    const { apiKey, timestamp, nonce, signature, data } = envelope;

    if (!apiKey || typeof apiKey !== 'string') {
      throw new Error('SECURITY_REJECT: Missing or invalid apiKey');
    }
    if (!timestamp || typeof timestamp !== 'number') {
      throw new Error('SECURITY_REJECT: Missing or invalid timestamp');
    }
    if (nonce === undefined || nonce === null) {
      throw new Error('SECURITY_REJECT: Missing nonce');
    }
    if (!signature || typeof signature !== 'string') {
      throw new Error('SECURITY_REJECT: Missing or invalid signature');
    }
    if (!data || typeof data !== 'object') {
      throw new Error('SECURITY_REJECT: Missing or invalid data payload');
    }

    // 1. Clock Drift Check
    const now = Date.now();
    if (Math.abs(now - timestamp) > this.maxDriftMs) {
      throw new Error(`SECURITY_REJECT: Timestamp drift exceeded threshold (${Math.abs(now - timestamp)}ms)`);
    }

    // 2. Monotonic Nonce Check (Anti-Replay)
    let parsedNonce;
    try {
      parsedNonce = BigInt(nonce);
    } catch {
      throw new Error('SECURITY_REJECT: Invalid non-numeric nonce format');
    }

    const lastWatermark = this.receivedWatermarks.get(apiKey) || 0n;
    if (parsedNonce <= lastWatermark) {
      throw new Error(`SECURITY_REJECT: Nonce replay or inversion detected (Got ${parsedNonce}, required > ${lastWatermark})`);
    }

    // 3. Cryptographic Signature Verification (Constant-Time)
    const canonical = this._canonicalize(apiKey, timestamp, nonce, data);
    const expectedSig = crypto
      .createHmac('sha256', this.clusterSecret)
      .update(canonical)
      .digest('hex');

    const sigBuf = Buffer.from(signature, 'hex');
    const expBuf = Buffer.from(expectedSig, 'hex');

    if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
      throw new Error('SECURITY_REJECT: Cryptographic signature mismatch');
    }

    // Commit watermark upon successful verification
    this.receivedWatermarks.set(apiKey, parsedNonce);

    const authenticatedUserId = this.deriveUserId(apiKey);

    return {
      apiKey,
      userId: authenticatedUserId,
      data
    };
  }
}

module.exports = { SecurityGate };