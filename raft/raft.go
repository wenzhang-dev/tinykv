// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
    "math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
    "github.com/pingcap-incubator/tinykv/log"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

const (
    campaignElection CampaignType = "CampaignElection"
)

type CampaignType string

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
    randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

    rand *rand.Rand
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {

    log.SetLevel(log.LOG_LEVEL_NONE)

	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
    r := &Raft {
        id: c.ID,
        Term: 0,
        Lead: None,
        Prs: make(map[uint64]*Progress),
        electionTimeout: c.ElectionTick,
        heartbeatTimeout: c.HeartbeatTick,
    }

    r.rand = rand.New(rand.NewSource(int64(c.ID)))

    // TODO
    for _, p := range c.peers {
        r.Prs[p] = &Progress{}
    }

    r.becomeFollower(r.Term, None)

    log.Infof("#%x newRaft", r.id)

	return r
}

func (r *Raft) quorum() int {
    return len(r.Prs) / 2 + 1
}

func (r *Raft) send(m pb.Message) {
    m.From = r.id
    // proposals are a way to forward to the leader and should be treated as local message
    if m.MsgType != pb.MessageType_MsgPropose {
        m.Term = r.Term
    }
    r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
    r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to})
}

func (r *Raft) tickElection() {
    r.electionElapsed++

    if r.electionElapsed >= r.randomizedElectionTimeout {

        r.electionElapsed = 0
        r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
    }
}

func (r* Raft) tickHeartbeat() {
    r.heartbeatElapsed++

    if r.heartbeatElapsed >= r.heartbeatTimeout {

        r.heartbeatElapsed = 0
        r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
    }
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
    switch r.State {
    case StateFollower, StateCandidate:
        r.tickElection()
    case StateLeader:
        r.tickHeartbeat()
    }
}

func (r *Raft) reset(term uint64) {
    if r.Term != term {
        r.Term = term
        r.Vote = None
    }
    r.Lead = None

    r.electionElapsed = 0
    r.heartbeatElapsed = 0
    r.randomizedElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)

    r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
    log.Infof("#%x state: %s->%s, item: %d->%d, lead: %x", r.id, r.State, StateFollower, r.Term, term, lead)

    r.reset(term)
    r.Lead = lead
    r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
    log.Infof("#%x state: %s->%s, item: %d", r.id, r.State, StateCandidate, r.Term)

    r.reset(r.Term + 1)
    r.Vote = r.id
    r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
    log.Infof("#%x state: %s->%s, term: %d", r.id, r.State, StateCandidate, r.Term)

    r.reset(r.Term)
    r.Lead = r.id
    r.State = StateLeader
}

func (r *Raft) stepFollower(m pb.Message) error {
    switch m.MsgType {
    case pb.MessageType_MsgAppend:
        r.electionElapsed = 0
        r.Lead = m.From
        r.handleAppendEntries(m)
    case pb.MessageType_MsgRequestVote:
        if r.Vote == None || r.Vote == m.From {
            r.electionElapsed = 0
            r.Vote = m.From
            r.send(
                pb.Message{
                    MsgType: pb.MessageType_MsgRequestVoteResponse,
                    To: m.From,
                },
            )
            log.Infof("#%x vote #%x", r.id, m.From)
        } else {
            r.send(
                pb.Message{
                    MsgType: pb.MessageType_MsgRequestVoteResponse,
                    To: m.From,
                    Reject: true,
                },
            )
            log.Infof("#%x rejected vote #%x", r.id, m.From)
        }
    default:
        // TODO
    }

    return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
    switch m.MsgType {
    case pb.MessageType_MsgHeartbeat:
        r.becomeFollower(m.Term, m.From)
        r.handleHeartbeat(m)
    case pb.MessageType_MsgAppend:
        r.becomeFollower(m.Term, m.From)
        r.handleAppendEntries(m)
    case pb.MessageType_MsgRequestVoteResponse:
        granted := r.poll(m.From, !m.Reject)
        switch r.quorum() {
        case granted:
            // majority nodes grant vote
            r.becomeLeader()
            // TODO bcast append
        case len(r.votes) - granted:
            // majority nodes reject vote
            r.becomeFollower(r.Term, None)
        }
    default:
        // TODO
    }

    return nil
}

func (r* Raft) stepLeader(m pb.Message) error {
    switch m.MsgType {
    case pb.MessageType_MsgBeat:
        return r.bcastHeartbeat()
    default:
        // TOOD:
    }
    return nil
}

// bcastHeartbeat sends RPC from leader, without entries to all the peers
func (r *Raft) bcastHeartbeat() error {
    for id := range r.Prs {
        if id == r.id {
            continue
        }
        r.sendHeartbeat(id)
    }

    return nil
}

func (r *Raft) campaign(t CampaignType) error {
    // 1. become candidate
    r.becomeCandidate()

    // 2. vote self
    if r.quorum() == r.poll(r.id, true) {
        r.becomeLeader()
        return nil
    }

    // 3. send vote rpc
    for id := range r.Prs {
        if id == r.id {
            continue
        }

        log.Infof("#%x sent vote request", r.id)
        r.send(pb.Message{To: id, MsgType: pb.MessageType_MsgRequestVote})
    }

    return nil
}

func (r *Raft) poll(id uint64, vote bool) int {
    if vote {
        log.Infof("#%x received vote from %x at term %d", r.id, id, r.Term)
    } else {
        log.Infof("#%x received vote rejection from %x at term %d", r.id, id, r.Term)
    }

    if _, ok := r.votes[id]; !ok {
        r.votes[id] = vote
    }

    granted := 0
    for _, vv := range r.votes {
        if vv {
            granted++
        }
    }

    return granted
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
    if m.MsgType == pb.MessageType_MsgHup {
        if r.State == StateLeader {
            log.Infof("#%x ignoring MsgHup message because already leader", r.id)
            return nil
        }

        log.Infof("#%x is starting a new election at term %d", r.id, r.Term)
        return r.campaign(campaignElection)
    }


    switch {
    case m.Term == 0:
        // local message
    case m.Term > r.Term:
        log.Infof("#%x received a higher term msg from #%x", r.id, m.From)
        r.becomeFollower(m.Term, m.From)
    case m.Term < r.Term:
        // TODO: maybe reject
        return nil
    }

	switch r.State {
	case StateFollower:
        if err := r.stepFollower(m); err != nil {
            return err
        }
	case StateCandidate:
        if err := r.stepCandidate(m); err != nil {
            return err
        }
	case StateLeader:
        if err := r.stepLeader(m); err != nil {
            return err
        }
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
