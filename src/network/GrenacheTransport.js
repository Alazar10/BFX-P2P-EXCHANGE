'use strict';

const Link = require('grenache-nodejs-link');
const { BinaryPeerRPCServer, BinaryPeerRPCClient } = require('./BinaryRPCTransports');

class GrenacheTransport {
  constructor(grapeUrl = 'http://127.0.0.1:30001', port = 1337) {
    this.grapeUrl = grapeUrl;
    this.port = port;

    this.link = new Link({ grape: this.grapeUrl });

    // RPC Server Peer
    this.peerServer = new BinaryPeerRPCServer(this.link, {});
    this.peerServer.init();

    // RPC Client Peer (for forwarding / inter-node Raft messages)
    this.peerClient = new BinaryPeerRPCClient(this.link, {});
    this.peerClient.init();

    this.service = null;
    this.requestHandler = null;
    this.announceInterval = null;
  }

  /**
   * Registers the request handler callback.
   * Safe to call before or after start().
   */
  onRequest(handler) {
    this.requestHandler = handler;
    if (this.service) {
      this._bindRequestHandler();
    }
  }

  _bindRequestHandler() {
    this.service.removeAllListeners('request');
    this.service.on('request', async (rid, key, payload, handlerCallback) => {
      try {
        if (!this.requestHandler) {
          return handlerCallback.reply('NO_HANDLER_ATTACHED', null);
        }
        const response = await this.requestHandler(payload);
        handlerCallback.reply(null, response);
      } catch (err) {
        handlerCallback.reply(err.message || 'ENGINE_INTERNAL_ERROR', null);
      }
    });
  }

  /**
   * Initializes WebSocket transport and announces service on Grape DHT
   */
  start(serviceKeys = ['rpc_order_engine']) {
    this.link.start();
    this.service = this.peerServer.transport('server', {
      maxBuffer: 1024 * 1024
    });

    this.service.listen(this.port);

    if (this.requestHandler) {
      this._bindRequestHandler();
    }

    const keys = Array.isArray(serviceKeys) ? serviceKeys : [serviceKeys];
    for (const key of keys) {
      this.link.announce(key, this.service.port, {});
    }

    this.announceInterval = setInterval(() => {
      for (const key of keys) {
        this.link.announce(key, this.service.port, {});
      }
    }, 1000);
  }

  /**
   * Sends an RPC payload to a specific key or peer
   */
  sendPeer(peerKey, payload, timeout = 5000) {
    const serviceKey = peerKey.startsWith('rpc_node_') ? peerKey : `rpc_node_${peerKey}`;
    return new Promise((resolve, reject) => {
      this.peerClient.request(serviceKey, payload, { timeout }, (err, data) => {
        if (err) return reject(new Error(typeof err === 'string' ? err : err.message));
        resolve(data);
      });
    });
  }

  stop() {
    if (this.announceInterval) clearInterval(this.announceInterval);
    if (this.service) this.service.stop();
    if (this.peerServer) this.peerServer.stop();
    if (this.peerClient) this.peerClient.stop();
    if (this.link) this.link.stop();
  }
}

module.exports = { GrenacheTransport };