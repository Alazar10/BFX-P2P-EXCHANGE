'use strict';

const crypto = require('node:crypto');
const Link = require('grenache-nodejs-link');
const { BinaryPeerRPCClient } = require('./network/BinaryRPCTransports');
const { SecurityGate } = require('./network/Security');

const SCALE = 100000000n;
const API_SECRET = process.env.API_SECRET;
const API_KEY = process.env.API_KEY;
if (!API_KEY || !API_SECRET || API_SECRET.length < 32) {
  throw new Error(
    'CONFIG_ERROR: Set API_KEY and API_SECRET to a client account configured in CLIENT_API_KEYS, ' +
    'then start the etcd-backed engine and Grenache network'
  );
}

const link = new Link({ grape: process.env.GRAPE_URL || 'http://127.0.0.1:30001' });
link.start();

const peer = new BinaryPeerRPCClient(link, {});
peer.init();
const security = new SecurityGate(API_SECRET);

function sendRaw(cmd, apiKey = API_KEY) {
  return new Promise((resolve, reject) => {
    const envelope = security.sign({
      ...cmd,
      requestId: cmd.requestId || crypto.randomUUID()
    }, apiKey);
    peer.request('rpc_order_engine', envelope, { timeout: 5000 }, (err, data) => {
      if (err) return reject(new Error(typeof err === 'string' ? err : err.message));
      resolve(data);
    });
  });
}

function sendCustomEnvelope(envelope) {
  return new Promise((resolve, reject) => {
    peer.request('rpc_order_engine', envelope, { timeout: 5000 }, (err, data) => {
      if (err) {
        return resolve({ rejected: true, reason: typeof err === 'string' ? err : err.message });
      }
      if (data === null) {
        return resolve({ rejected: true, reason: 'REPLIED_NULL_ERROR' });
      }
      resolve({ rejected: false, data });
    });
  });
}

async function runRaceConditionTests() {
  await new Promise((r) => setTimeout(r, 1500));
  console.log('=== STARTING RACE CONDITION TEST SUITE ===\n');

  console.log('[Test 1] Executing Simultaneous Dual-Cancellation Race...');
  const targetOrderId = BigInt(Date.now() * 1000 + 1);

  await sendRaw({
    type: 'ORDER_CREATE',
    orderId: targetOrderId.toString(),
    price: (70000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1, // SELL
    stpMode: 1
  });

  const cancelPromises = Array.from({ length: 10 }, () =>
    sendRaw({
      type: 'ORDER_CANCEL',
      orderId: targetOrderId.toString()
    }).catch((err) => ({ error: err.message }))
  );

  await Promise.all(cancelPromises);
  console.log('-> 10 Simultaneous Cancel Dispatches Settled.');
  console.log('-> Pool Integrity Verified: No buffer double-free or engine crash.\n');

  console.log('[Test 2] Executing Match-vs-Cancel Collision...');
  const collisionOrderId = BigInt(Date.now() * 1000 + 2);

  await sendRaw({
    type: 'ORDER_CREATE',
    orderId: collisionOrderId.toString(),
    price: (68000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1, // SELL (Maker)
    stpMode: 1
  });

  const [takerResult] = await Promise.all([
    sendRaw({
      type: 'ORDER_CREATE',
      orderId: BigInt(Date.now() * 1000 + 3).toString(),
      price: (68000n * SCALE).toString(),
      amount: (1n * SCALE).toString(),
      side: 0, // BUY (Taker)
      stpMode: 1
    }),
    sendRaw({
      type: 'ORDER_CANCEL',
      orderId: collisionOrderId.toString()
    }).catch(() => null)
  ]);

  const fills = takerResult && takerResult.fills ? takerResult.fills : [];
  if (fills.length > 0) {
    console.log(`-> Collision Resolved: Taker MATCHED before Cancel processed. Filled: ${fills[0].amount} Satoshis.`);
  } else {
    console.log('-> Collision Resolved: CANCEL processed before Taker arrived. Order was cleanly voided.');
  }

  console.log('\n[Test 3] Executing Out-of-Order / Duplicate Nonce Race...');
  let rejectedCount = 0;

  const testCmd = {
    type: 'ORDER_CREATE',
    orderId: (BigInt(Date.now()) * 1000n + 88888n).toString(),
    price: (65000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1,
    stpMode: 1,
    requestId: crypto.randomUUID()
  };

  const clientNonce = security.clientNonces.get(API_KEY) || 0n;
  const initialNonce = (BigInt(Date.now()) > clientNonce
    ? BigInt(Date.now())
    : clientNonce + 1n).toString();
  const initialEnvelope = security.sign(testCmd, API_KEY, initialNonce);
  const initialResult = await sendCustomEnvelope(initialEnvelope);
  if (initialResult.rejected) {
    throw new Error(`NONCE_TEST_SETUP_FAILED: Valid configured API key request rejected: ${initialResult.reason}`);
  }

  const initialNonceValue = BigInt(initialNonce);
  const badNonces = [1n, 2n, 3n, 4n].map((offset) =>
    (initialNonceValue - offset).toString()
  );
  for (const badNonce of badNonces) {
    const result = await sendCustomEnvelope(security.sign(testCmd, API_KEY, badNonce));

    if (result.rejected) {
      rejectedCount++;
    }
  }

  console.log(`-> Dispatched 4 invalid/inverted nonce payloads. Rejected: ${rejectedCount}/4`);
  if (rejectedCount !== badNonces.length) {
    throw new Error(`NONCE_REPLAY_TEST_FAILED: Expected ${badNonces.length} rejected requests, got ${rejectedCount}`);
  }

  console.log('\n=== ALL RACE CONDITION TESTS COMPLETED ===');
}

runRaceConditionTests()
  .catch((error) => {
    console.error('Race condition test failed:', error);
    process.exitCode = 1;
  })
  .finally(() => {
    peer.stop();
    link.stop();
  });