'use strict';

const { PeerRPCServer, PeerRPCClient } = require('grenache-nodejs-ws');
const Link = require('grenache-nodejs-link');

class GrenacheTransport {
  /**
   * @param {string} grapeUrl - e.g. 'http://127.0.0.1:30001'
   * @param {number} servicePort - e.g. 1337
   * @param {string} serviceKey - e.g. 'rpc_order_engine'
   */
  constructor(grapeUrl, servicePort, serviceKey) {
    this.grapeUrl = grapeUrl;
    this.servicePort = servicePort;
    this.serviceKey = serviceKey;

    this.link = new Link({ grape: this.grapeUrl });
    this.link.startAnnouncing = null;
  }

  init() {
    this.link.start();
    this.peerServer = new PeerRPCServer(this.link, {});
    this.peerServer.init();

    this.peerClient = new PeerRPCClient(this.link, {});
    this.peerClient.init();

    this.service = this.peerServer.transport('server');
    this.service.listen(this.servicePort);

    this.announceInterval = setInterval(() => {
      this.link.announce(this.serviceKey, this.service.port, {});
    }, 1000);
  }

  /**
   * Registers deterministic RPC handler on the Grenache WebSocket transport
   * @param {function(object): Promise<object>} handler
   */
  onRequest(handler) {
    this.service.on('request', async (rid, key, payload, handlerCallback) => {
      try {
        const response = await handler(payload);
        handlerCallback.reply(null, response);
      } catch (err) {
        handlerCallback.reply(err.message || 'ENGINE_INTERNAL_ERROR', null);
      }
    });
  }

  send(payload, timeout = 5000) {
    return new Promise((resolve, reject) => {
      this.peerClient.request(this.serviceKey, payload, { timeout }, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  }

  stop() {
    if (this.announceInterval) clearInterval(this.announceInterval);
    if (this.link) this.link.stop();
  }
}

module.exports = { GrenacheTransport };