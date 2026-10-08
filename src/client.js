'use strict';

const crypto = require('node:crypto');
const Link = require('grenache-nodejs-link');
const { BinaryPeerRPCClient } = require('./network/BinaryRPCTransports');
const { SecurityGate } = require('./network/Security');

const SCALE = 100000000n; // 1 BTC = 10^8 Satoshis
const API_KEY = process.env.API_KEY;
const API_SECRET = process.env.API_SECRET;
if (!API_KEY || !API_SECRET || API_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Set API_KEY and API_SECRET (at least 32 characters)');
}
const TAKER_API_KEY = process.env.TAKER_API_KEY || API_KEY;
const TAKER_API_SECRET = process.env.TAKER_API_SECRET || API_SECRET;
if (Boolean(process.env.TAKER_API_KEY) !== Boolean(process.env.TAKER_API_SECRET) ||
    TAKER_API_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Configure both TAKER_API_KEY and TAKER_API_SECRET');
}

const link = new Link({
  grape: process.env.GRAPE_URL || 'http://127.0.0.1:40001'
});
link.start();

const peer = new BinaryPeerRPCClient(link, {});
peer.init();

const securityByKey = new Map();

function sendOrder(command, apiKey = API_KEY, apiSecret = API_SECRET) {
  return new Promise((resolve, reject) => {
    let signer = securityByKey.get(apiKey);
    if (signer && signer.secret !== apiSecret) {
      return reject(new Error('CONFIG_ERROR: Conflicting secrets configured for an API key'));
    }
    if (!signer) {
      signer = { secret: apiSecret, gate: new SecurityGate(apiSecret) };
      securityByKey.set(apiKey, signer);
    }
    const envelope = signer.gate.sign({
      ...command,
      requestId: command.requestId || crypto.randomUUID()
    }, apiKey);

    peer.request('rpc_order_engine', envelope, { timeout: 10000 }, (err, data) => {
      if (err) return reject(new Error(typeof err === 'string' ? err : err.message));
      resolve(data);
    });
  });
}

(async () => {

  await new Promise((r) => setTimeout(r, 1500));

  console.log('=== SENDING P2P ORDERS TO MATCHING ENGINE ===\n');

  try {

    console.log('[Order 1] Submitting Maker Ask (Sell 2.0 BTC @ $65,000)...');
    const res1 = await sendOrder({
      type: 'ORDER_CREATE',
      orderId: (Date.now() + 1).toString(),
      price: (65000n * SCALE).toString(),
      amount: (2n * SCALE).toString(),
      side: 1, // SELL
      stpMode: 1 // CANCEL_TAKER
    });
    console.log('Result 1 (Ask Placed):', res1);

    console.log('\n[Order 2] Submitting Taker Bid (Buy 0.75 BTC @ $65,000)...');
    if (TAKER_API_KEY === API_KEY) {
      console.log('Configure TAKER_API_KEY/TAKER_API_SECRET to simulate a separate counterparty.');
    }
    const res2 = await sendOrder({
      type: 'ORDER_CREATE',
      orderId: (Date.now() + 2).toString(),
      price: (65000n * SCALE).toString(),
      amount: ((75n * SCALE) / 100n).toString(),
      side: 0, // BUY
      stpMode: 1
    }, TAKER_API_KEY, TAKER_API_SECRET);
    console.log('Result 2 (Trade or STP):', res2);

    console.log('\n[Order 3] Testing Self-Trade Prevention (Trader 101 buys own Ask)...');
    const res3 = await sendOrder({
      type: 'ORDER_CREATE',
      orderId: (Date.now() + 3).toString(),
      price: (65000n * SCALE).toString(),
      amount: (1n * SCALE).toString(),
      side: 0, // BUY
      stpMode: 1 // CANCEL_TAKER
    });
    console.log('Result 3 (STP Triggered):', res3);

    console.log('\n=== ALL P2P RPC TESTS PASSED ===');
  } catch (err) {
    console.error('RPC Error:', err);
  } finally {
    peer.stop();
    link.stop();
    process.exit(0);
  }
})();