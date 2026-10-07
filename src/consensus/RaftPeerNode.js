'use strict';

const EventEmitter = require('node:events');

const NodeRole = Object.freeze({
  LEADER: 'LEADER',
  FOLLOWER: 'FOLLOWER',
  CANDIDATE: 'CANDIDATE'
});

class RaftPeerNode extends EventEmitter {
  /**
   * @param {string} nodeId - Unique peer ID (e.g. "peer_1")
   * @param {string[]} clusterPeers - List of peer IDs in cluster
   * @param {import('./Sequencer').Sequencer} sequencer
   * @param {import('../network/GrenacheTransport').GrenacheTransport} transport
   */
  constructor(nodeId, clusterPeers, sequencer, transport) {
    super();
    this.nodeId = nodeId;
    this.clusterPeers = clusterPeers;
    this.sequencer = sequencer;
    this.transport = transport;

    this.role = NodeRole.FOLLOWER;
    this.currentTerm = 0;
    this.votedFor = null;
    this.leaderId = null;

    this.heartbeatInterval = 1000;
    this.electionTimeoutMin = 2500;
    this.electionTimeoutMax = 4000;
    this.timer = null;

    this._resetElectionTimer();
  }

  _resetElectionTimer() {
    if (this.timer) clearTimeout(this.timer);
    const timeout = Math.floor(
      Math.random() * (this.electionTimeoutMax - this.electionTimeoutMin) + this.electionTimeoutMin
    );
    this.timer = setTimeout(() => this._startElection(), timeout);
  }

  _startElection() {
    this.role = NodeRole.CANDIDATE;
    this.currentTerm++;
    this.votedFor = this.nodeId;
    console.log(`[Raft] ${this.nodeId}: Election timeout reached. Starting election for Term ${this.currentTerm}...`);

    let votes = 1; // Vote for self
    this._resetElectionTimer();

    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      
      this.transport.send({
        action: 'RAFT_REQUEST_VOTE',
        term: this.currentTerm,
        candidateId: this.nodeId,
        lastSeqId: this.sequencer.currentSequence.toString()
      }).then((res) => {
        if (res && res.voteGranted && this.role === NodeRole.CANDIDATE) {
          votes++;
          if (votes > Math.floor(this.clusterPeers.length / 2)) {
            this._becomeLeader();
          }
        }
      }).catch(() => {});
    }
  }

  _becomeLeader() {
    this.role = NodeRole.LEADER;
    this.leaderId = this.nodeId;
    if (this.timer) clearTimeout(this.timer);
    console.log(`\n>>> [Raft] CLUSTER LEADER ELECTED: ${this.nodeId} (Term ${this.currentTerm}) <<<\n`);

    this.heartbeatTimer = setInterval(() => this._broadcastHeartbeat(), this.heartbeatInterval);
  }

  _broadcastHeartbeat() {
    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      this.transport.send({
        action: 'RAFT_HEARTBEAT',
        term: this.currentTerm,
        leaderId: this.nodeId,
        lastSeqId: this.sequencer.currentSequence.toString()
      }).catch(() => {});
    }
  }

  async submitTransaction(command) {
    if (this.role !== NodeRole.LEADER) {
      if (!this.leaderId) throw new Error('NO_LEADER_ELECTED_YET');
      return this.transport.send({
        action: 'FORWARD_ORDER_TO_LEADER',
        leaderId: this.leaderId,
        command
      });
    }

    const nextSeqId = this.sequencer.currentSequence + 1n;
    const logEntry = { ...command, seqId: nextSeqId.toString() };

    let acks = 1;
    const replicationPromises = this.clusterPeers
      .filter((id) => id !== this.nodeId)
      .map((id) =>
        this.transport.send({
          action: 'RAFT_APPEND_ENTRIES',
          term: this.currentTerm,
          leaderId: this.nodeId,
          entry: logEntry
        }).then((res) => {
          if (res && res.success) acks++;
        }).catch(() => {})
      );

    await Promise.all(replicationPromises);

    if (acks <= Math.floor(this.clusterPeers.length / 2)) {
      throw new Error('CONSENSUS_QUORUM_LOST: Order rejected to prevent split-brain divergence.');
    }

    const result = this.sequencer.process(command);

    this.transport.send({
      action: 'RAFT_COMMIT',
      seqId: nextSeqId.toString()
    }).catch(() => {});

    return result;
  }

  handleRaftMessage(msg) {
    if (msg.action === 'RAFT_REQUEST_VOTE') {
      if (msg.term > this.currentTerm) {
        this.currentTerm = msg.term;
        this.role = NodeRole.FOLLOWER;
        this.votedFor = msg.candidateId;
        this._resetElectionTimer();
        return { voteGranted: true };
      }
      return { voteGranted: false };
    }

    if (msg.action === 'RAFT_HEARTBEAT') {
      if (msg.term >= this.currentTerm) {
        this.currentTerm = msg.term;
        this.role = NodeRole.FOLLOWER;
        this.leaderId = msg.leaderId;
        this._resetElectionTimer();
        return { success: true };
      }
      return { success: false };
    }

    if (msg.action === 'RAFT_APPEND_ENTRIES') {
      this._resetElectionTimer();

      this.pendingEntry = msg.entry;
      return { success: true };
    }

    if (msg.action === 'RAFT_COMMIT') {
      if (this.pendingEntry && this.pendingEntry.seqId === msg.seqId) {

        this.sequencer.process(this.pendingEntry);
        this.pendingEntry = null;
        return { success: true };
      }
    }

    return { error: 'UNKNOWN_RAFT_ACTION' };
  }
}

module.exports = { RaftPeerNode, NodeRole };