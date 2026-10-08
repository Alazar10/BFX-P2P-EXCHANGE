'use strict';

const Ws = require('ws');
const { decode, encode } = require('@msgpack/msgpack');
const { PeerRPCClient, PeerRPCServer } = require('grenache-nodejs-ws');
const BaseTransportRPCClient = require('grenache-nodejs-ws/lib/TransportRPCClient');
const BaseTransportRPCServer = require('grenache-nodejs-ws/lib/TransportRPCServer');

const MAX_FRAME_BYTES = 1024 * 1024;

function parseFrame(transport, data) {
  try {
    if (data.byteLength > MAX_FRAME_BYTES) {
      throw new Error('RPC frame exceeds maximum size');
    }
    const frame = decode(data, { maxStrLength: MAX_FRAME_BYTES, maxBinLength: MAX_FRAME_BYTES });
    if (!Array.isArray(frame) || frame.length !== 3) {
      throw new Error('RPC frame must be a three-element array');
    }
    return frame;
  } catch (error) {
    transport.emit('parse-error', error);
    return null;
  }
}

class BinaryTransportRPCClient extends BaseTransportRPCClient {
  format(value) {
    const frame = Buffer.from(encode(value));
    if (frame.byteLength > MAX_FRAME_BYTES) {
      throw new Error('RPC_FRAME_TOO_LARGE');
    }
    return frame;
  }

  parse(data) {
    return parseFrame(this, data);
  }

  getSocket(conf) {
    const dest = `${conf.secure ? 'wss' : 'ws'}://${conf.dest}/ws`;
    return new Ws(dest, {
      ...(conf.secure || {}),
      maxPayload: MAX_FRAME_BYTES,
      perMessageDeflate: false
    });
  }
}

class BinaryTransportRPCServer extends BaseTransportRPCServer {
  format(value) {
    const frame = Buffer.from(encode(value));
    if (frame.byteLength > MAX_FRAME_BYTES) {
      throw new Error('RPC_FRAME_TOO_LARGE');
    }
    return frame;
  }

  parse(data) {
    return parseFrame(this, data);
  }

  getSocket(opts) {
    return new Ws.Server({
      ...opts,
      maxPayload: MAX_FRAME_BYTES,
      perMessageDeflate: false
    });
  }
}

class BinaryPeerRPCClient extends PeerRPCClient {
  getTransportClass() {
    return BinaryTransportRPCClient;
  }
}

class BinaryPeerRPCServer extends PeerRPCServer {
  getTransportClass() {
    return BinaryTransportRPCServer;
  }
}

module.exports = {
  BinaryPeerRPCClient,
  BinaryPeerRPCServer,
  BinaryTransportRPCClient,
  BinaryTransportRPCServer,
  MAX_FRAME_BYTES
};
