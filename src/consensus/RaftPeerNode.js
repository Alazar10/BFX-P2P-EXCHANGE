'use strict';

const EventEmitter = require('node:events');

const NodeRole = Object.freeze({
  LEADER: 'LEADER',
  FOLLOWER: 'FOLLOWER',
  CANDIDATE: 'CANDIDATE'
});

class RaftPeerNode extends EventEmitter {
  constructor(nodeId, clusterPeers, sequencer, transport, security = null) {
    super();
    this.nodeId = nodeId;
    this.clusterPeers = clusterPeers;
    this.sequencer = sequencer;
    this.transport = transport;
    this.security = security;

    this.role = NodeRole.FOLLOWER;
    this.currentTerm = 0;
    this.votedFor = null;
    this.leaderId = null;

    // Ordered log of uncommitted entries: Map<seqIdStr, entry>
    this.uncommittedLog = new Map();
    this.committedProposals = new Map();
    this.proposalCounter = 0n;
    this.commitIndex = this.sequencer.currentSequence;
    this.submissionTail = Promise.resolve();

    this.sequencer.wal.replay((record) => {
      if (record.type === 'RAFT_PREPARE') {
        const proposalIndex = BigInt(record.proposalIndex);
        if (proposalIndex > this.proposalCounter) this.proposalCounter = proposalIndex;
        if (BigInt(record.seqId) > this.sequencer.currentSequence) {
          const previous = this.uncommittedLog.get(record.seqId);
          if (!previous || Number(record.term) > Number(previous.term) ||
              (Number(record.term) === Number(previous.term) &&
               proposalIndex > BigInt(previous.proposalIndex))) {
            this.uncommittedLog.set(record.seqId, record.entry);
          }
        }
      } else if (record.type === 'RAFT_TERM_VOTE') {
        this.currentTerm = Math.max(this.currentTerm, Number(record.term));
        this.votedFor = record.votedFor || null;
      }
    });

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

  _sendPeer(peerId, message) {
    const wireMessage = JSON.parse(JSON.stringify(message, (_, value) =>
      typeof value === 'bigint' ? value.toString() : value
    ));
    const payload = this.security
      ? this.security.signClusterMessage(this.nodeId, wireMessage)
      : wireMessage;
    return this.transport.sendPeer(peerId, payload);
  }

  _persistTermVote() {
    this.sequencer.wal.append({
      type: 'RAFT_TERM_VOTE',
      term: this.currentTerm,
      votedFor: this.votedFor
    });
  }

  _startElection() {
    this.role = NodeRole.CANDIDATE;
    this.currentTerm++;
    this.votedFor = this.nodeId;
    this._persistTermVote();
    let votes = 1;
    this._resetElectionTimer();

    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      this._sendPeer(peerId, {
        action: 'RAFT_REQUEST_VOTE',
        term: this.currentTerm,
        candidateId: this.nodeId,
        lastSeqId: this.sequencer.currentSequence.toString()
      }).then((res) => {
        if (res && res.term > this.currentTerm) {
          this.currentTerm = res.term;
          this.role = NodeRole.FOLLOWER;
          this.votedFor = null;
          this.leaderId = null;
          this._persistTermVote();
          this._resetElectionTimer();
          return;
        }
        if (res && res.voteGranted && this.role === NodeRole.CANDIDATE &&
            this.currentTerm === res.term) {
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
    this.heartbeatTimer = setInterval(() => this._broadcastHeartbeat(), this.heartbeatInterval);
  }

  _broadcastHeartbeat() {
    for (const peerId of this.clusterPeers) {
      if (peerId === this.nodeId) continue;
      this._sendPeer(peerId, {
        action: 'RAFT_HEARTBEAT',
        term: this.currentTerm,
        leaderId: this.nodeId,
        lastSeqId: this.sequencer.currentSequence.toString(),
      }).catch(() => {});
    }
  }

  /**
   * Two-Phase Consensus: Propose -> Quorum Ack -> Commit & Mutate State
   */
  submitTransaction(command) {
    const operation = this.submissionTail.then(() => this._submitTransaction(command));
    this.submissionTail = operation.catch(() => {});
    return operation;
  }

  async _submitTransaction(command) {
    if (this.role !== NodeRole.LEADER) {
      if (!this.leaderId) throw new Error('NO_LEADER_ELECTED_YET');
      return this._sendPeer(this.leaderId, {
        action: 'FORWARD_ORDER_TO_LEADER',
        command
      });
    }

    const assignedSeq = this.sequencer.currentSequence + 1n;
    const proposalIndex = (++this.proposalCounter).toString();
    const logEntry = {
      ...command,
      seqId: assignedSeq.toString(),
      timestamp: Date.now(),
      proposalIndex,
      term: this.currentTerm
    };

    // Staged records survive a crash but are ignored by state recovery until committed.
    this.sequencer.wal.append({
      type: 'RAFT_PREPARE',
      seqId: logEntry.seqId,
      proposalIndex,
      term: this.currentTerm,
      leaderId: this.nodeId,
      entry: logEntry
    });
    this.uncommittedLog.set(logEntry.seqId, logEntry);

    let acks = 1; // Leader vote
    const replicationPromises = this.clusterPeers
      .filter((id) => id !== this.nodeId)
      .map((id) =>
        this._sendPeer(id, {
          action: 'RAFT_APPEND_ENTRIES',
          term: this.currentTerm,
          leaderId: this.nodeId,
          proposalIndex,
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

    if (this.role !== NodeRole.LEADER || logEntry.term !== this.currentTerm) {
      this.uncommittedLog.delete(logEntry.seqId);
      throw new Error('LEADERSHIP_CHANGED: Proposal was not committed');
    }

    const execResult = this.sequencer.applyCommitted(logEntry);
    this.commitIndex = assignedSeq;
    this.uncommittedLog.delete(logEntry.seqId);

    const commitMessages = [];
    for (const id of this.clusterPeers) {
      if (id !== this.nodeId) {
        commitMessages.push(this._sendPeer(id, {
          action: 'RAFT_COMMIT',
          term: this.currentTerm,
          leaderId: this.nodeId,
          seqId: logEntry.seqId,
          proposalIndex
        }).catch(() => null));
      }
    }
    await Promise.all(commitMessages);

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
        this.leaderId = null;
        this._persistTermVote();
      }

      // Safe Vote Rule: Term must match, not yet voted for another, and candidate log >= local log
      if (
        msg.term === this.currentTerm &&
        (this.votedFor === null || this.votedFor === msg.candidateId) &&
        candidateLastSeq >= localLastSeq
      ) {
        voteGranted = true;
        this.votedFor = msg.candidateId;
        this._persistTermVote();
        this._resetElectionTimer();
      }

      return { voteGranted, term: this.currentTerm };
    }

    if (msg.action === 'RAFT_HEARTBEAT') {
      if (msg.term > this.currentTerm) {
        this.currentTerm = msg.term;
        this.votedFor = null;
        this._persistTermVote();
      }
      if (msg.term >= this.currentTerm) {
        this.role = NodeRole.FOLLOWER;
        this.leaderId = msg.leaderId;
        this._resetElectionTimer();
        return { success: true };
      }
      return { success: false };
    }

    if (msg.action === 'RAFT_APPEND_ENTRIES') {
      if (msg.term < this.currentTerm) return { success: false, term: this.currentTerm };
      if (msg.term > this.currentTerm) {
        this.currentTerm = msg.term;
        this.votedFor = null;
        this._persistTermVote();
      }
      if (BigInt(msg.entry.seqId) <= this.sequencer.currentSequence) {
        return { success: false, alreadyCommitted: true };
      }
      if (msg.term === this.currentTerm && this.leaderId && this.leaderId !== msg.leaderId) {
        return { success: false, conflictingLeader: true };
      }

      const existing = this.uncommittedLog.get(msg.entry.seqId);
      if (existing && (existing.term > msg.term ||
          (existing.term === msg.term &&
           BigInt(existing.proposalIndex) > BigInt(msg.proposalIndex)))) {
        return { success: false, staleProposal: true };
      }

      this._resetElectionTimer();
      this.role = NodeRole.FOLLOWER;
      this.leaderId = msg.leaderId;
      const stagedEntry = {
        ...msg.entry,
        term: msg.term,
        proposalIndex: msg.proposalIndex
      };
      this.sequencer.wal.append({
        type: 'RAFT_PREPARE',
        seqId: stagedEntry.seqId,
        proposalIndex: msg.proposalIndex,
        term: msg.term,
        leaderId: msg.leaderId,
        entry: stagedEntry
      });
      this.uncommittedLog.set(msg.entry.seqId, stagedEntry);
      return { success: true };
    }

    if (msg.action === 'RAFT_COMMIT') {
      if (msg.term < this.currentTerm) return { success: false, term: this.currentTerm };
      if (msg.term > this.currentTerm) {
        this.currentTerm = msg.term;
        this.votedFor = null;
        this._persistTermVote();
      }
      this.leaderId = msg.leaderId;
      const proposal = `${msg.term}:${msg.proposalIndex}`;
      this.committedProposals.set(msg.seqId, proposal);
      const targetEntry = this.uncommittedLog.get(msg.seqId);
      if (!targetEntry || targetEntry.term !== msg.term ||
          targetEntry.proposalIndex !== msg.proposalIndex) {
        return { success: false, missingEntry: true };
      }

      while (true) {
        const nextTarget = (this.sequencer.currentSequence + 1n).toString();
        const nextEntry = this.uncommittedLog.get(nextTarget);
        const committedProposal = this.committedProposals.get(nextTarget);
        if (!nextEntry || committedProposal !== `${nextEntry.term}:${nextEntry.proposalIndex}`) break;

        this.sequencer.applyCommitted(nextEntry);
        this.uncommittedLog.delete(nextTarget);
        this.committedProposals.delete(nextTarget);
      }
      this.commitIndex = this.sequencer.currentSequence;
      return { success: true, currentSeq: this.sequencer.currentSequence.toString() };
    }

    if (msg.action === 'FORWARD_ORDER_TO_LEADER') {
      return this.submitTransaction(msg.command);
    }

    return { error: 'UNKNOWN_RAFT_ACTION' };
  }
}

module.exports = { RaftPeerNode, NodeRole };