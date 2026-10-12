'use strict';

const crypto = require('node:crypto');

class SecurityGate {
  /**
   * @param {string} clusterSecret - Secret used for signing unless an API-key map is supplied
   * @param {number} maxDriftMs - Maximum allowed timestamp drift in ms (default 5000)
   * @param {Record<string, string>|null} apiKeySecrets - Per-client signing secrets
   */
  constructor(clusterSecret, maxDriftMs = 5000, apiKeySecrets = null) {
    if ((!clusterSecret || typeof clusterSecret !== 'string') &&
        (!clusterSecret || typeof clusterSecret !== 'object' || Array.isArray(clusterSecret))) {
      throw new Error('SECURITY_INIT_ERROR: cluster secrets must be a string or peer-secret map');
    }
    this.clusterSecret = clusterSecret;
    this.maxDriftMs = maxDriftMs;
    this.apiKeySecrets = apiKeySecrets;

    // Outbound client counter: Map<apiKey, bigint>
    this.clientNonces = new Map();

    // Inbound verification watermark: Map<apiKey, bigint>
    this.receivedWatermarks = new Map();
    this.receivedClusterNonces = new Map();
    this.clusterNonceQueue = [];
    this.clusterNonceQueueHead = 0;
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
    const serializedData = JSON.stringify(data, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    );
    return `${apiKey}:${timestamp}:${nonce}:${serializedData}`;
  }

  _stableSerialize(value) {
    if (typeof value === 'bigint') return JSON.stringify(value.toString());
    if (Array.isArray(value)) {
      return `[${value.map((item) => this._stableSerialize(item)).join(',')}]`;
    }
    if (value && typeof value === 'object') {
      const entries = Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${this._stableSerialize(value[key])}`);
      return `{${entries.join(',')}}`;
    }
    return JSON.stringify(value);
  }

  signClusterMessage(nodeId, message) {
    const timestamp = Date.now();
    const nonce = crypto.randomUUID();
    const canonical = `${nodeId}:${timestamp}:${nonce}:${this._stableSerialize(message)}`;
    const signingSecret = this._clusterSecretFor(nodeId);
    if (!signingSecret) throw new Error('CLUSTER_AUTH_ERROR: No secret configured for this node');
    const signature = crypto
      .createHmac('sha256', signingSecret)
      .update(canonical)
      .digest('hex');

    return { clusterMessage: true, nodeId, timestamp, nonce, signature, message };
  }

  verifyClusterMessage(envelope, allowedNodeIds) {
    if (!envelope || envelope.clusterMessage !== true ||
        typeof envelope.nodeId !== 'string' ||
        !allowedNodeIds.includes(envelope.nodeId) ||
        typeof envelope.timestamp !== 'number' ||
        typeof envelope.nonce !== 'string' ||
        typeof envelope.signature !== 'string' ||
        !envelope.message || typeof envelope.message !== 'object') {
      throw new Error('CLUSTER_AUTH_REJECT: Malformed or untrusted peer envelope');
    }

    const now = Date.now();
    if (Math.abs(now - envelope.timestamp) > this.maxDriftMs) {
      throw new Error('CLUSTER_AUTH_REJECT: Peer message timestamp expired');
    }
    const signingSecret = this._clusterSecretFor(envelope.nodeId);
    if (!signingSecret) {
      throw new Error('CLUSTER_AUTH_REJECT: No secret configured for claimed peer');
    }

    const replayKey = `${envelope.nodeId}:${envelope.nonce}`;
    while (this.clusterNonceQueueHead < this.clusterNonceQueue.length &&
        now - this.clusterNonceQueue[this.clusterNonceQueueHead][1] > this.maxDriftMs) {
      const [expiredKey] = this.clusterNonceQueue[this.clusterNonceQueueHead++];
      this.receivedClusterNonces.delete(expiredKey);
    }
    if (this.clusterNonceQueueHead > 1024 &&
        this.clusterNonceQueueHead * 2 > this.clusterNonceQueue.length) {
      this.clusterNonceQueue = this.clusterNonceQueue.slice(this.clusterNonceQueueHead);
      this.clusterNonceQueueHead = 0;
    }
    if (this.receivedClusterNonces.has(replayKey)) {
      throw new Error('CLUSTER_AUTH_REJECT: Replayed peer message');
    }
    if (this.receivedClusterNonces.size >= 100000) {
      throw new Error('CLUSTER_AUTH_REJECT: Peer replay window is at capacity');
    }

    const canonical = `${envelope.nodeId}:${envelope.timestamp}:${envelope.nonce}:${this._stableSerialize(envelope.message)}`;
    const expectedSig = crypto
      .createHmac('sha256', signingSecret)
      .update(canonical)
      .digest('hex');
    const sigBuf = Buffer.from(envelope.signature, 'hex');
    const expBuf = Buffer.from(expectedSig, 'hex');
    if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
      throw new Error('CLUSTER_AUTH_REJECT: Peer signature mismatch');
    }

    this.receivedClusterNonces.set(replayKey, now);
    this.clusterNonceQueue.push([replayKey, now]);
    return { nodeId: envelope.nodeId, message: envelope.message };
  }

  _clusterSecretFor(nodeId) {
    return typeof this.clusterSecret === 'string'
      ? this.clusterSecret
      : this.clusterSecret[nodeId];
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
    const signingSecret = this.apiKeySecrets
      ? this.apiKeySecrets[apiKey]
      : this.clusterSecret;
    if (!signingSecret) throw new Error('SECURITY_SIGN_ERROR: Unknown API key');
    const signature = crypto
      .createHmac('sha256', signingSecret)
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
    const signingSecret = this.apiKeySecrets
      ? this.apiKeySecrets[apiKey]
      : this.clusterSecret;
    if (!signingSecret) throw new Error('SECURITY_REJECT: Unknown API key');
    const canonical = this._canonicalize(apiKey, timestamp, nonce, data);
    const expectedSig = crypto
      .createHmac('sha256', signingSecret)
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