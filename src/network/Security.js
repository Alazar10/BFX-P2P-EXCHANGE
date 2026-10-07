'use strict';

const crypto = require('node:crypto');

class SecurityGate {
  /**
   * @param {string} clusterSecret
   */
  constructor(clusterSecret) {
    if (typeof clusterSecret !== 'string' || !clusterSecret) {
      throw new TypeError('SecurityGate: clusterSecret must be a non-empty string');
    }
    this.clusterSecret = clusterSecret;
    this.nonces = new Map();
    this.localNonceCounter = 0n;
  }

  generateMonotonicNonce() {
    const timePrefix = BigInt(Date.now()) * 1000000n;
    return (timePrefix + (++this.localNonceCounter)).toString();
  }

  sign(payload, apiKey) {
    const timestamp = Date.now();
    const nonce = this.generateMonotonicNonce();
    const bodyStr = typeof payload === 'string' ? payload : JSON.stringify(payload);

    const canonical = `${apiKey}:${timestamp}:${nonce}:${bodyStr}`;
    const signature = crypto.createHmac('sha256', this.clusterSecret).update(canonical).digest('hex');

    return {
      apiKey,
      timestamp,
      nonce,
      signature,
      data: payload
    };
  }

  verify(envelope) {
    const { apiKey, timestamp, nonce, signature, data } = envelope;

    if (!apiKey || !timestamp || !nonce || !signature) {
      throw new Error('SECURITY_REJECT: Missing cryptographic envelope fields');
    }

    if (Math.abs(Date.now() - timestamp) > 5000) {
      throw new Error('SECURITY_REJECT: Request timestamp drift exceeded (+/- 5000ms)');
    }

    const nonceVal = BigInt(nonce);
    const lastNonce = this.nonces.get(apiKey) || 0n;
    if (nonceVal <= lastNonce) {
      throw new Error(`SECURITY_REJECT: Nonce ${nonceVal} <= Last ${lastNonce} (Replay Defense)`);
    }

    const bodyStr = typeof data === 'string' ? data : JSON.stringify(data);
    const canonical = `${apiKey}:${timestamp}:${nonce}:${bodyStr}`;
    const expectedSig = crypto.createHmac('sha256', this.clusterSecret).update(canonical).digest('hex');

    const sigBuf = Buffer.from(signature, 'hex');
    const expBuf = Buffer.from(expectedSig, 'hex');

    if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
      throw new Error('SECURITY_REJECT: Cryptographic signature mismatch');
    }

    this.nonces.set(apiKey, nonceVal);
    return true;
  }
}

module.exports = { SecurityGate };