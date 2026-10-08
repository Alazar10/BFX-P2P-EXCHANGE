'use strict';

const EventEmitter = require('node:events');

const NodeRole = Object.freeze({
  LEADER: 'LEADER',
  FOLLOWER: 'FOLLOWER',
  CANDIDATE: 'CANDIDATE'
});

class RaftPeerNode extends EventEmitter {
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

    // Ordered log of uncommitted entries: Map<seqIdStr, entry>
    this.uncommittedLog = new Map();
    this.commitIndex = this.sequencer.currentSequence;
    this.nextSeqId = this.sequencer.currentSequence;

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
    let votes = 1;
    this._resetElectionTimer();

    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      this.transport.sendPeer(peerId, {
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
    this.nextSeqId = this.sequencer.currentSequence;
    if (this.timer) clearTimeout(this.timer);
    this.heartbeatTimer = setInterval(() => this._broadcastHeartbeat(), this.heartbeatInterval);
  }

  _broadcastHeartbeat() {
    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      this.transport.sendPeer(peerId, {
        action: 'RAFT_HEARTBEAT',
        term: this.currentTerm,
        leaderId: this.nodeId,
        lastSeqId: this.sequencer.currentSequence.toString(),
        commitIndex: this.commitIndex.toString()
      }).catch(() => {});
    }
  }

  /**
   * Two-Phase Consensus: Propose -> Quorum Ack -> Commit & Mutate State
   */
  async submitTransaction(command) {
    if (this.role !== NodeRole.LEADER) {
      if (!this.leaderId) throw new Error('NO_LEADER_ELECTED_YET');
      return this.transport.sendPeer(this.leaderId, {
        action: 'FORWARD_ORDER_TO_LEADER',
        command
      });
    }

    // 1. Assign strict next sequence without mutating state yet
    const assignedSeq = ++this.nextSeqId;
    const logEntry = {
      ...command,
      seqId: assignedSeq.toString(),
      timestamp: Date.now()
    };

    // 2. Stage locally in uncommitted log
    this.uncommittedLog.set(logEntry.seqId, logEntry);

    // 3. Replicate to Quorum
    let acks = 1; // Leader vote
    const replicationPromises = this.clusterPeers
      .filter((id) => id !== this.nodeId)
      .map((id) =>
        this.transport.sendPeer(id, {
          action: 'RAFT_APPEND_ENTRIES',
          term: this.currentTerm,
          leaderId: this.nodeId,
          entry: logEntry
        }).then((res) => {
          if (res && res.success) acks++;
        }).catch(() => {})
      );

    await Promise.all(replicationPromises);

    // If quorum lost, fail cleanly without having mutated state
    if (acks <= Math.floor(this.clusterPeers.length / 2)) {
      this.uncommittedLog.delete(logEntry.seqId);
      throw new Error('QUORUM_LOST: Order rejected to prevent split-brain state.');
    }

    // 4. Commit and Apply locally on leader
    const execResult = this.sequencer.applyCommitted(logEntry);
    this.commitIndex = assignedSeq;
    this.uncommittedLog.delete(logEntry.seqId);

    // 5. Notify followers of commit
    for (const id of this.clusterPeers) {
      if (id !== this.nodeId) {
        this.transport.sendPeer(id, {
          action: 'RAFT_COMMIT',
          seqId: logEntry.seqId
        }).catch(() => {});
      }
    }

    return execResult;
  }

  handleRaftMessage(msg) {
    if (msg.action === 'RAFT_REQUEST_VOTE') {
      let voteGranted = false;
      const candidateLastSeq = BigInt(msg.lastSeqId || '0');
      const localLastSeq = this.sequencer.currentSequence;

      if (msg.term > this.currentTerm) {
        this.currentTerm = msg.term;
        this.role = NodeRole.FOLLOWER;
        this.votedFor = null;
      }

      // Safe Vote Rule: Term must match, not yet voted for another, and candidate log >= local log
      if (
        msg.term === this.currentTerm &&
        (this.votedFor === null || this.votedFor === msg.candidateId) &&
        candidateLastSeq >= localLastSeq
      ) {
        voteGranted = true;
        this.votedFor = msg.candidateId;
        this._resetElectionTimer();
      }

      return { voteGranted, term: this.currentTerm };
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
      // Store in uncommitted buffer
      this.uncommittedLog.set(msg.entry.seqId, msg.entry);
      return { success: true };
    }

    if (msg.action === 'RAFT_COMMIT') {
      // Contiguous Commit Drain: apply entries strictly in monotonic order
      let targetSeq = BigInt(msg.seqId);
      
      // If entry exists, ensure we apply contiguously
      while (true) {
        const nextTarget = (this.sequencer.currentSequence + 1n).toString();
        const nextEntry = this.uncommittedLog.get(nextTarget);
        if (!nextEntry) break;

        this.sequencer.applyCommitted(nextEntry);
        this.uncommittedLog.delete(nextTarget);
      }
      return { success: true, currentSeq: this.sequencer.currentSequence.toString() };
    }

    if (msg.action === 'FORWARD_ORDER_TO_LEADER') {
      return this.submitTransaction(msg.command);
    }

    return { error: 'UNKNOWN_RAFT_ACTION' };
  }
}

module.exports = { RaftPeerNode, NodeRole };