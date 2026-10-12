'use strict';

const { EventEmitter } = require('node:events');
const crypto = require('node:crypto');

const LOG_BATCH_SIZE = 256;
const REQUEST_RETRIES = 64;
const MAX_CACHED_RESULTS = 1000;
const MAX_POLL_INTERVAL_MS = 5000;

function encode(value) {
  return Buffer.from(value, 'utf8').toString('base64');
}

function decode(value) {
  return Buffer.from(value, 'base64').toString('utf8');
}

function stableSerialize(value) {
  if (typeof value === 'bigint') return JSON.stringify(value.toString());
  if (Array.isArray(value)) return `[${value.map(stableSerialize).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort()
      .map((key) => `${JSON.stringify(key)}:${stableSerialize(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function stringify(value) {
  return JSON.stringify(value, (_, nested) =>
    typeof nested === 'bigint' ? nested.toString() : nested
  );
}

class EtcdOrderLog extends EventEmitter {
  constructor(sequencer, {
    endpoints = process.env.ETCD_ENDPOINTS,
    prefix = process.env.ETCD_PREFIX || '/p2p-exchange/v1',
    authToken = process.env.ETCD_AUTH_TOKEN,
    requestTimeoutMs = 3000,
    fetchImpl = globalThis.fetch
  } = {}) {
    super();
    if (typeof fetchImpl !== 'function') {
      throw new Error('ETCD_CONFIG_ERROR: Node.js fetch support is required');
    }
    this.endpoints = (endpoints || '').split(',').map((value) => value.trim()).filter(Boolean);
    if (this.endpoints.length === 0) {
      throw new Error('ETCD_CONFIG_ERROR: Set ETCD_ENDPOINTS to one or more etcd v3 gateway URLs');
    }
    if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs <= 0) {
      throw new Error('ETCD_CONFIG_ERROR: requestTimeoutMs must be a positive integer');
    }
    for (const endpoint of this.endpoints) {
      const url = new URL(endpoint);
      const localHost = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
      if (!['http:', 'https:'].includes(url.protocol) ||
          (url.protocol !== 'https:' && !localHost)) {
        throw new Error('ETCD_CONFIG_ERROR: Remote etcd endpoints must use HTTPS');
      }
      if (!localHost && !authToken) {
        throw new Error('ETCD_CONFIG_ERROR: Remote etcd endpoints require ETCD_AUTH_TOKEN');
      }
    }
    if (!prefix.startsWith('/') || prefix.includes('//') || prefix.endsWith('/')) {
      throw new Error('ETCD_CONFIG_ERROR: ETCD_PREFIX must be an absolute key prefix without a trailing slash');
    }

    this.sequencer = sequencer;
    this.prefix = prefix;
    this.sequenceKey = `${prefix}/sequence`;
    this.logPrefix = `${prefix}/log/`;
    this.requestPrefix = `${prefix}/requests/`;
    this.orderIdPrefix = `${prefix}/order-ids/`;
    this.authToken = authToken;
    this.requestTimeoutMs = requestTimeoutMs;
    this.fetchImpl = fetchImpl;
    this.endpointCursor = 0;
    this.resultsByRequestId = new Map();
    this.catchUpPromise = null;
    this.timer = null;
    this.stopped = true;
    this.pollIntervalMs = 100;
    this.currentPollIntervalMs = 100;
  }

  async start(pollIntervalMs = 100) {
    if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs <= 0) {
      throw new Error('ETCD_CONFIG_ERROR: poll interval must be a positive integer');
    }
    await this.catchUp();
    this.stopped = false;
    this.pollIntervalMs = pollIntervalMs;
    this.currentPollIntervalMs = pollIntervalMs;
    this._schedulePoll();
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  _schedulePoll() {
    if (!this.stopped && !this.timer) {
      this.timer = setTimeout(async () => {
        this.timer = null;
        try {
          await this.catchUp();
          this.currentPollIntervalMs = this.pollIntervalMs;
        } catch (error) {
          this.emit('sync-error', error);
          this.currentPollIntervalMs = Math.min(
            this.currentPollIntervalMs * 2,
            MAX_POLL_INTERVAL_MS
          );
        }
        if (!this.stopped) this._schedulePoll();
      }, this.currentPollIntervalMs);
      this.timer.unref();
    }
  }

  async submit(command, requestId) {
    if (typeof requestId !== 'string' ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(requestId)) {
      throw new Error('INVALID_REQUEST_ID: requestId must be a UUID v4');
    }
    const requestKey = `${this.requestPrefix}${requestId}`;
    const commandHash = crypto.createHash('sha256').update(stableSerialize(command)).digest('hex');

    for (let attempt = 0; attempt < REQUEST_RETRIES; attempt++) {
      await this.catchUp();
      const prior = await this._readValue(requestKey);
      if (prior) {
        const priorRequest = JSON.parse(prior.value);
        if (priorRequest.commandHash !== commandHash) {
          throw new Error('IDEMPOTENCY_CONFLICT: requestId was already used for a different command');
        }
        await this.catchUp();
        return this.resultsByRequestId.get(requestId) || {
          seqId: BigInt(priorRequest.seqId),
          fills: [],
          duplicate: true
        };
      }

      const head = await this._readHead();
      if (head.seqId !== this.sequencer.currentSequence) continue;
      this.sequencer.validateCommand(command);
      const orderIdKey = command.type === 'ORDER_CREATE'
        ? `${this.orderIdPrefix}${command.orderId.toString()}`
        : null;
      if (orderIdKey && await this._readValue(orderIdKey)) {
        throw new Error('DUPLICATE_ORDER_ID: Order ID has already been used');
      }

      const seqId = head.seqId + 1n;
      const entry = {
        ...command,
        seqId: seqId.toString(),
        requestId,
        timestamp: Date.now()
      };
      const requestValue = JSON.stringify({
        seqId: seqId.toString(),
        commandHash
      });
      const logKey = this._logKey(seqId);
      const transaction = await this._post('/v3/kv/txn', {
        compare: [
          {
            key: encode(this.sequenceKey),
            target: 'MOD',
            result: 'EQUAL',
            modRevision: head.modRevision
          },
          {
            key: encode(requestKey),
            target: 'VERSION',
            result: 'EQUAL',
            version: '0'
          },
          ...(orderIdKey ? [{
            key: encode(orderIdKey),
            target: 'VERSION',
            result: 'EQUAL',
            version: '0'
          }] : [])
        ],
        success: [
          {
            requestPut: {
              key: encode(this.sequenceKey),
              value: encode(seqId.toString())
            }
          },
          {
            requestPut: {
              key: encode(logKey),
              value: encode(stringify(entry))
            }
          },
          {
            requestPut: {
              key: encode(requestKey),
              value: encode(requestValue)
            }
          },
          ...(orderIdKey ? [{
            requestPut: {
              key: encode(orderIdKey),
              value: encode(seqId.toString())
            }
          }] : [])
        ],
        failure: []
      });
      if (!transaction.succeeded) continue;

      await this.catchUp();
      const result = this.resultsByRequestId.get(requestId);
      if (!result) {
        throw new Error(`ETCD_APPLY_PENDING: Committed sequence ${seqId} is not yet applied locally`);
      }
      return result;
    }

    throw new Error('ETCD_CONTENTION: Could not append command after repeated sequence conflicts');
  }

  catchUp() {
    if (this.catchUpPromise) return this.catchUpPromise;
    this.catchUpPromise = this._catchUp().finally(() => {
      this.catchUpPromise = null;
    });
    return this.catchUpPromise;
  }

  async _catchUp() {
    const head = await this._readHead();
    if (head.seqId < this.sequencer.currentSequence) {
      throw new Error('ETCD_LOG_REGRESSION: etcd sequence is behind local durable state');
    }

    while (this.sequencer.currentSequence < head.seqId) {
      const start = this.sequencer.currentSequence + 1n;
      const response = await this._post('/v3/kv/range', {
        key: encode(this._logKey(start)),
        rangeEnd: encode(`${this.logPrefix}\xff`),
        limit: LOG_BATCH_SIZE,
        sortOrder: 'ASCEND',
        sortTarget: 'KEY'
      });
      const entries = response.kvs || [];
      if (entries.length === 0) {
        throw new Error(`ETCD_LOG_GAP: Missing committed entry at sequence ${start}`);
      }

      for (const item of entries) {
        const entry = JSON.parse(decode(item.value));
        if (BigInt(entry.seqId) !== this.sequencer.currentSequence + 1n) {
          throw new Error(`ETCD_LOG_GAP: Expected ${this.sequencer.currentSequence + 1n}, received ${entry.seqId}`);
        }
        const result = this.sequencer.applyCommitted(entry);
        this._cacheResult(entry.requestId, result);
      }
    }
  }

  async _readHead() {
    let item = await this._readValue(this.sequenceKey);
    if (!item) {
      const existingLog = await this._post('/v3/kv/range', {
        key: encode(this.logPrefix),
        rangeEnd: encode(`${this.logPrefix}\xff`),
        limit: 1
      });
      if (existingLog.kvs && existingLog.kvs.length > 0) {
        throw new Error('ETCD_SEQUENCE_MISSING: Refusing to reset sequence while log entries exist');
      }
      await this._post('/v3/kv/txn', {
        compare: [{
          key: encode(this.sequenceKey),
          target: 'VERSION',
          result: 'EQUAL',
          version: '0'
        }],
        success: [{
          requestPut: {
            key: encode(this.sequenceKey),
            value: encode('0')
          }
        }],
        failure: []
      });
      item = await this._readValue(this.sequenceKey);
    }
    if (!item || !/^(0|[1-9][0-9]*)$/.test(item.value)) {
      throw new Error('ETCD_SEQUENCE_INVALID: Sequence key is missing or malformed');
    }
    return {
      seqId: BigInt(item.value),
      modRevision: item.modRevision
    };
  }

  async _readValue(key) {
    const response = await this._post('/v3/kv/range', { key: encode(key) });
    const item = response.kvs && response.kvs[0];
    if (!item) return null;
    return {
      value: decode(item.value),
      modRevision: item.mod_revision || item.modRevision
    };
  }

  _logKey(seqId) {
    return `${this.logPrefix}${seqId.toString().padStart(78, '0')}`;
  }

  _cacheResult(requestId, result) {
    if (!requestId) return;
    this.resultsByRequestId.set(requestId, result);
    if (this.resultsByRequestId.size > MAX_CACHED_RESULTS) {
      this.resultsByRequestId.delete(this.resultsByRequestId.keys().next().value);
    }
  }

  async _post(path, body) {
    const start = this.endpointCursor++ % this.endpoints.length;
    let lastError;
    for (let offset = 0; offset < this.endpoints.length; offset++) {
      const endpoint = this.endpoints[(start + offset) % this.endpoints.length].replace(/\/+$/, '');
      const headers = { 'content-type': 'application/json' };
      if (this.authToken) headers.authorization = `Bearer ${this.authToken}`;
      try {
        const response = await this.fetchImpl(`${endpoint}${path}`, {
          method: 'POST',
          headers,
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(this.requestTimeoutMs)
        });
        const responseText = await response.text();
        if (!response.ok) {
          if (response.status >= 500 || response.status === 429) {
            lastError = new Error(`ETCD_HTTP_${response.status}: ${responseText.slice(0, 256)}`);
            continue;
          }
          throw new Error(`ETCD_HTTP_${response.status}: ${responseText.slice(0, 256)}`);
        }
        let parsed;
        try {
          parsed = JSON.parse(responseText);
        } catch (error) {
          throw new Error(`ETCD_INVALID_RESPONSE: ${error.message}`);
        }
        if (parsed.error) throw new Error(`ETCD_REQUEST_ERROR: ${parsed.error}`);
        return parsed;
      } catch (error) {
        if (error.message.startsWith('ETCD_HTTP_4') ||
            error.message.startsWith('ETCD_REQUEST_ERROR') ||
            error.message.startsWith('ETCD_INVALID_RESPONSE')) {
          throw error;
        }
        lastError = error;
      }
    }
    throw new Error(`ETCD_UNAVAILABLE: ${lastError ? lastError.message : 'No healthy endpoints'}`);
  }
}

module.exports = { EtcdOrderLog };
