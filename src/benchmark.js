'use strict';

const Link = require('grenache-nodejs-link');
const { PeerRPCClient } = require('grenache-nodejs-ws');
const { SecurityGate } = require('./network/Security');

const SCALE = 100000000n;
const API_KEY = process.env.API_KEY;
const API_SECRET = process.env.API_SECRET;
if (!API_KEY || !API_SECRET || API_SECRET.length < 32) {
  throw new Error('CONFIG_ERROR: Set API_KEY and API_SECRET (at least 32 characters)');
}
const TOTAL_ORDERS = 5000;
const CONCURRENCY = 20;

const link = new Link({ grape: process.env.GRAPE_URL || 'http://127.0.0.1:40001' });
link.start();

const peer = new PeerRPCClient(link, { maxActiveKeyDests: 10 });
peer.init();
const security = new SecurityGate(API_SECRET);

let orderCounter = 0n;

function sendBenchmarkOrder(i) {
  return new Promise((resolve, reject) => {
    const isBuy = i % 2 === 0;
    const userId = isBuy ? '202' : '101';
    const price = 65000n * SCALE;
    const amount = (1n * SCALE) / 100n; // 0.01 BTC

    const cmd = {
      type: 'ORDER_CREATE',
      orderId: (++orderCounter).toString(),
      userId,
      price: price.toString(),
      amount: amount.toString(),
      side: isBuy ? 0 : 1,
      stpMode: 0
    };

    const envelope = security.sign(cmd, API_KEY);
    const start = process.hrtime.bigint();

    peer.request('rpc_order_engine', envelope, { timeout: 10000 }, (err, data) => {
      const end = process.hrtime.bigint();
      const latencyMs = Number(end - start) / 1_000_000;

      if (err) {
        return reject(new Error(typeof err === 'string' ? err : err.message || 'RPC_FAIL'));
      }
      resolve(latencyMs);
    });
  });
}

(async () => {
  console.log('Waiting 2s for Grape DHT route settlement...');
  await new Promise((r) => setTimeout(r, 2000));

  console.log(`\n======================================================`);
  console.log(` BENCHMARK: Sending ${TOTAL_ORDERS} signed orders (Concurrency: ${CONCURRENCY})`);
  console.log(`======================================================\n`);

  const latencies = [];
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  async function worker(queue) {
    while (queue.length > 0) {
      const idx = queue.pop();
      try {
        const latency = await sendBenchmarkOrder(idx);
        latencies.push(latency);
      } catch (err) {
        failed++;
      }
      completed++;
      if (completed % 500 === 0 || completed === TOTAL_ORDERS) {
        process.stdout.write(`Processed ${completed}/${TOTAL_ORDERS} orders (Errors: ${failed})...\r`);
      }
    }
  }

  const taskQueue = Array.from({ length: TOTAL_ORDERS }, (_, i) => i);
  const workers = Array.from({ length: CONCURRENCY }, () => worker(taskQueue));

  await Promise.all(workers);

  const totalDurationSec = (Date.now() - startTime) / 1000;

  if (latencies.length === 0) {
    console.error('\nBenchmark failed: 0 successful requests. Is the engine listening on PORT=1337?');
    peer.stop();
    link.stop();
    process.exit(1);
  }

  latencies.sort((a, b) => a - b);

  const p50 = latencies[Math.floor(latencies.length * 0.50)].toFixed(3);
  const p95 = latencies[Math.floor(latencies.length * 0.95)].toFixed(3);
  const p99 = latencies[Math.floor(latencies.length * 0.99)].toFixed(3);
  const throughput = (latencies.length / totalDurationSec).toFixed(2);

  console.log(`\n\n--- BENCHMARK RESULTS ---`);
  console.log(`Successful Orders: ${latencies.length}`);
  console.log(`Failed Orders:     ${failed}`);
  console.log(`Total Time:        ${totalDurationSec.toFixed(2)}s`);
  console.log(`Throughput:        ${throughput} orders/sec`);
  console.log(`Latency p50:       ${p50} ms`);
  console.log(`Latency p95:       ${p95} ms`);
  console.log(`Latency p99:       ${p99} ms`);
  console.log(`-------------------------\n`);

  peer.stop();
  link.stop();
  process.exit(0);
})();