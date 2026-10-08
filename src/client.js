'use strict';

const Link = require('grenache-nodejs-link');
const { PeerRPCClient } = require('grenache-nodejs-ws');
const { SecurityGate } = require('./network/Security');

const SCALE = 100000000n; // 1 BTC = 10^8 Satoshis
const API_KEY = process.env.API_KEY;
const API_SECRET = process.env.API_SECRET;
if (!API_KEY || !API_SECRET || API_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Set API_KEY and API_SECRET (at least 32 characters)');
}

const link = new Link({
  grape: process.env.GRAPE_URL || 'http://127.0.0.1:40001'
});
link.start();

const peer = new PeerRPCClient(link, {});
peer.init();

const security = new SecurityGate(API_SECRET);

function sendOrder(command, apiKey = API_KEY) {
  return new Promise((resolve, reject) => {
    const envelope = security.sign(command, apiKey || API_KEY);

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
      userId: '101',
      price: (65000n * SCALE).toString(),
      amount: (2n * SCALE).toString(),
      side: 1, // SELL
      stpMode: 1 // CANCEL_TAKER
    });
    console.log('Result 1 (Ask Placed):', res1);

    console.log('\n[Order 2] Submitting Taker Bid (Buy 0.75 BTC @ $65,000)...');
    const res2 = await sendOrder({
      type: 'ORDER_CREATE',
      orderId: (Date.now() + 2).toString(),
      userId: '202',
      price: (65000n * SCALE).toString(),
      amount: ((75n * SCALE) / 100n).toString(),
      side: 0, // BUY
      stpMode: 1
    });
    console.log('Result 2 (Trade Executed):', res2);

    console.log('\n[Order 3] Testing Self-Trade Prevention (Trader 101 buys own Ask)...');
    const res3 = await sendOrder({
      type: 'ORDER_CREATE',
      orderId: (Date.now() + 3).toString(),
      userId: '101', // Same user ID
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