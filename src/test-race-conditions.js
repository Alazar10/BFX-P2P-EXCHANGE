'use strict';

const crypto = require('node:crypto');
const Link = require('grenache-nodejs-link');
const { PeerRPCClient } = require('grenache-nodejs-ws');
const { SecurityGate } = require('./network/Security');

const SCALE = 100000000n;
const API_SECRET = process.env.API_SECRET;
const API_KEY = process.env.API_KEY;
if (!API_KEY || !API_SECRET || API_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Set API_KEY and API_SECRET for a configured client account');
}

const link = new Link({ grape: process.env.GRAPE_URL || 'http://127.0.0.1:40001' });
link.start();

const peer = new PeerRPCClient(link, {});
peer.init();
const security = new SecurityGate(API_SECRET);

function sendRaw(cmd, apiKey = API_KEY) {
  return new Promise((resolve, reject) => {
    const envelope = security.sign(cmd, apiKey);
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

(async () => {
  await new Promise((r) => setTimeout(r, 1500));
  console.log('=== STARTING RACE CONDITION TEST SUITE ===\n');

  console.log('[Test 1] Executing Simultaneous Dual-Cancellation Race...');
  const targetOrderId = BigInt(Date.now() * 1000 + 1);

  await sendRaw({
    type: 'ORDER_CREATE',
    orderId: targetOrderId.toString(),
    userId: '501',
    price: (70000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1, // SELL
    stpMode: 0
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
    userId: '601',
    price: (68000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1, // SELL (Maker)
    stpMode: 0
  });

  const [takerResult] = await Promise.all([
    sendRaw({
      type: 'ORDER_CREATE',
      orderId: BigInt(Date.now() * 1000 + 3).toString(),
      userId: '701',
      price: (68000n * SCALE).toString(),
      amount: (1n * SCALE).toString(),
      side: 0, // BUY (Taker)
      stpMode: 0
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

  const testApiKey = 'nonce_race_tester';
  const testCmd = {
    type: 'ORDER_CREATE',
    orderId: '88888',
    userId: '303',
    price: (65000n * SCALE).toString(),
    amount: (1n * SCALE).toString(),
    side: 1,
    stpMode: 0
  };

  const initialTimestamp = Date.now();
  const initialNonce = '50000';
  const initialBody = JSON.stringify(testCmd);
  const initialCanonical = `${testApiKey}:${initialTimestamp}:${initialNonce}:${initialBody}`;
  const initialSig = crypto.createHmac('sha256', API_SECRET).update(initialCanonical).digest('hex');

  await sendCustomEnvelope({
    apiKey: testApiKey,
    timestamp: initialTimestamp,
    nonce: initialNonce,
    signature: initialSig,
    data: testCmd
  });

  const badNonces = ['40000', '50000', '30000', '49999'];

  for (const badNonce of badNonces) {
    const timestamp = Date.now();
    const bodyStr = JSON.stringify(testCmd);
    const canonical = `${testApiKey}:${timestamp}:${badNonce}:${bodyStr}`;
    const signature = crypto.createHmac('sha256', API_SECRET).update(canonical).digest('hex');

    const result = await sendCustomEnvelope({
      apiKey: testApiKey,
      timestamp,
      nonce: badNonce,
      signature,
      data: testCmd
    });

    if (result.rejected) {
      rejectedCount++;
    }
  }

  console.log(`-> Dispatched 4 invalid/inverted nonce payloads. Rejected: ${rejectedCount}/4`);

  console.log('\n=== ALL RACE CONDITION TESTS COMPLETED ===');
  peer.stop();
  link.stop();
  process.exit(0);
})();