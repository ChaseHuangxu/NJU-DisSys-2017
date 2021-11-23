package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"
	"math/rand"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

type RaftState string

const (
	LEADER RaftState = "Leader"
	CANDIDATE RaftState = "Candidate"
	FOLLOWER RaftState = "Follower"
)

const HEARTBEATINTERVAL = 100 * time.Millisecond


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftLog struct {
	Command interface{}
	Term int
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	logs        []RaftLog

	// non persistent state
	state RaftState
	commitIndex int
	lastApplied int

	lastHBTime time.Time    // 上一次收到心跳的时间
	isAlive bool
	applyCh chan ApplyMsg

	// only leader
	nextIndex   []int
	matchIndex  []int
}


// Add obejct information to debug
func (rf *Raft) String() string {
	return fmt.Sprintf("[Server %d] Current Term: %d, Current State: %s, VotedFor: %d, CommitIndex: %d, " +
		"LastApplied: %d, isAlive: %v, LogLen: %v.",
		rf.me, rf.currentTerm, rf.state, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.isAlive, rf.logsLen())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// Only persist: currentTerm, votedFor, logs
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term  int
	VoteGranted  bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []RaftLog
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//

// Once become leader, need change raft_role_state and init leader's attr,
// including nextindex, matchindex
// state has changed, so should persist
// and don't forget send heartbeat immediately
func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	lastLogIndex := rf.lastLog().Index
	for i, _ := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.persist()
	//DPrintf("%s, become leader", rf)
	go rf.leaderSendAppendEntries(true)
}

// Become candidate, term++, raftstate changed, and vote to self, and persist
func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.persist()
}

// Become follower
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FOLLOWER
}

// the first log is sentinel to avoid log is empty
func (rf *Raft) lastLog() RaftLog {
	return rf.logs[rf.logsLen()]
}

func (rf *Raft) logsLen() int {
	return len(rf.logs) - 1
}

// check two server's log, whose log more newer
// by judge last log index, last log term
func moreUpToDate(lastLogIndex, lastLogTerm, otherLastLogIndex, otherLastLogTerm int) bool {
	if lastLogTerm > otherLastLogTerm {
		return true
	}
	if lastLogTerm == otherLastLogTerm && lastLogIndex >= otherLastLogIndex {
		return true
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Handle vote request, the goal is leader election
// check term, has voted, log newer, and so on...
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	// This server late, so become follower and change term to speed leader election
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm($$5.1)
	if args.Term < rf.currentTerm {
		return
	}
	// Reply false if has voted for others($$5.2)
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	// Reply false if requirer not up-to-date than this($$5.4)
	if !moreUpToDate(args.LastLogIndex, args.LastLogTerm, rf.lastLog().Index, rf.lastLog().Term){
		return
	}

	// Vote and update self attr
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	//DPrintf("%s voted for %d on term %d.", rf, args.CandidateId, rf.currentTerm)
	return
}

// Append Entries Handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%s receive append entries from %d", rf, args.LeaderId)
	rf.mu.Lock()
	rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm

	// if leader term ne current server term, reject, $$5.1
	if args.Term != rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.lastHBTime = time.Now()
			rf.becomeFollower(args.Term)
		}
		return
	}

	// record last heart beat time and ensure self role is follower
	rf.lastHBTime = time.Now()
	rf.becomeFollower(args.Term)

	// if have not last log or last log doesn't match, reject, $$5.3
	if args.PrevLogIndex > rf.logsLen() || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		// if last log doesn't match, delete this log and all that follow it
		if args.PrevLogIndex <= rf.logsLen() {
			rf.logs = rf.logs[0:args.PrevLogIndex]
		}
		return
	}

	// Add new logs and update self attr
	rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
	}

	//DPrintf("Args entries last command: %v", (interface{})(args.Entries[len(args.Entries) - 1].Command))
	DPrintf("%s append entries success, now log len: %v, last log command: %v.", rf, rf.logsLen(), rf.lastLog().Command)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (this *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := this.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// server start leader election
// first become candidate
// send vote request to other servers and receive result
// if timeout, end this procedure
func (rf *Raft) attemptLeaderElection(){
	rf.mu.Lock()
	rf.becomeCandidate()
	voteCnt := 1
	done := false
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLog().Index,
		LastLogTerm:  rf.lastLog().Term,
	}
	rf.mu.Unlock()

	//DPrintf("%s, start leader election.", rf)

	for serverId, _ := range rf.peers {
		if serverId == rf.me {
			continue
		}

		go func(serverId int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, args, &reply)
			if !ok{
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// current server state has changed, so exit.
			if done || rf.state != CANDIDATE || rf.currentTerm != args.Term {
				return
			}

			// Only allow vote in same term.
			if reply.Term != rf.currentTerm {
				if reply.Term > rf.currentTerm{
					done = true
					rf.becomeFollower(reply.Term)
				}
				return
			}

			if !reply.VoteGranted {
				return
			}

			// in here, rf.state is candidate
			voteCnt++
			//DPrintf("%s receive vote from %d, now receive vote num: %d/%d.", rf, serverId, voteCnt, len(rf.peers))
			if voteCnt > len(rf.peers) / 2 {
				done = true
				rf.becomeLeader()
			}
		}(serverId)
	}

	// if timeout, exit
	time.Sleep(HEARTBEATINTERVAL)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// has become leader or receive heartbeat from leader
	// now return directly
	if rf.state == FOLLOWER || rf.state == LEADER {
		return
	}

	// election failed, from candidate to follower
	done = true
	//DPrintf("%s attemp leader election failed, now term + 1.", rf)
	rf.becomeFollower(rf.currentTerm + 1)
}

// Leader server send logs and heartbeat to other servers
// Remember only process current term logs
func (rf *Raft) leaderSendAppendEntries(heartBeat bool) {
	rf.mu.Lock()
	if !rf.isAlive || rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()

	done := false
	
	for serverId, _ := range rf.peers {
		if serverId == rf.me {
			continue
		}

		// if leader has newer log or send heartbeat, then send msg to others
		if rf.lastLog().Index > rf.nextIndex[serverId] || heartBeat  {
			rf.mu.Lock()

			nextIndex := rf.nextIndex[serverId]
			//DPrintf("server %d next index %v.", serverId, rf.nextIndex[serverId])
			//DPrintf("current %d nextindex: %v, lastlogindex: %v", serverId, nextIndex, rf.lastLog().Index)
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if rf.lastLog().Index + 1 < nextIndex {
				nextIndex = rf.lastLog().Index
			}
			prevLog := rf.logs[nextIndex - 1]
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LeaderCommit: rf.commitIndex,
				Entries: make([]RaftLog, 0),
				PrevLogTerm: prevLog.Term,
				PrevLogIndex: prevLog.Index,
			}
			// Add logs according nextindex
			for idx := nextIndex; idx <= rf.logsLen(); idx++ {
				args.Entries = append(args.Entries, rf.logs[idx])
				//DPrintf("%s Send log entries: %v", rf, rf.logs[idx])
				//DPrintf("%s Send args entries: %v", rf, args.Entries[len(args.Entries) - 1])
			}
			DPrintf("%s Send log entries to %d, len: %v", rf, serverId, len(args.Entries))

			rf.mu.Unlock()

			go func(serverId int, args AppendEntriesArgs) {
				DPrintf("%s send append entries to %d.", rf, serverId)
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(serverId, args, &reply)
				if !ok{
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// if state has chaned, return
				if done || args.Term != rf.currentTerm || rf.state != LEADER {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					done = true
					return
				}

				if reply.Success {
					match := args.PrevLogIndex + len(args.Entries)
					rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
					rf.nextIndex[serverId] = max(rf.nextIndex[serverId], match + 1)
					//DPrintf("match: %v", match)
					//DPrintf("update %d nextindex: %v, matchindex: %v.", serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
				} else{
					// receiver reject, should decrease nextindex
					prevIndex := args.PrevLogIndex
					for prevIndex > 0 && rf.logs[prevIndex].Term == args.PrevLogTerm {
						prevIndex--
					}
					rf.nextIndex[serverId] = prevIndex + 1
					//DPrintf("server %d nextindex change to %v", serverId, rf.nextIndex[serverId])
				}

			}(serverId, args)

		}
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// Add new log and send to peers
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastLog().Index + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// return directly if current server is not leader
	if !isLeader {
		return index, term, isLeader
	}

	DPrintf("%s receive log from client, log index: %d.", rf, index)
	newRaftLog := RaftLog{
		Command: command,
		Term:    rf.currentTerm,
		Index:   index,
	}
	rf.logs = append(rf.logs, newRaftLog)
	rf.persist()
	go rf.leaderSendAppendEntries(false)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.isAlive = false
	DPrintf("%s killed.", rf)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]RaftLog, 0),
		state:       FOLLOWER,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		lastHBTime: time.Now(),
		isAlive: true,
		applyCh: applyCh,
	}

	rf.logs = append(rf.logs, RaftLog{
		Command: -1,
		Term:    0,
		Index:   0,
	})

	// Your initialization code here.
	
	// Election loop
	// If has not receive heartbeat from leader, attempt leader election periodic
	// Periodic interval should different to avoid all servers wake up and attempt leader election at the same time
	go func() {
		rand.Seed(time.Now().UnixNano())
		for rf.isAlive {
			sleepInterval := 30 + 10 * rand.Intn(10)
			time.Sleep(time.Duration(time.Duration(sleepInterval) * time.Millisecond))
			rf.mu.Lock()
			if rf.state != FOLLOWER {
				rf.mu.Unlock()
				continue
			}
			if time.Now().Sub(rf.lastHBTime) > 2 * HEARTBEATINTERVAL {
				rf.mu.Unlock()
				go rf.attemptLeaderElection()
				continue
			}
			rf.mu.Unlock()
		}
	}()
	
	// Leader send heartbeat loop
	// Periodic, interval is 100ms
	go func() {
		for rf.isAlive {
			time.Sleep(HEARTBEATINTERVAL)
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				continue
			}
			
			rf.mu.Unlock()
			go rf.leaderSendAppendEntries(true)
		}
	}()

	// Leader update self commitIndex loop
	// Periodic check
	go func() {
		for rf.isAlive {
			time.Sleep(time.Duration(20) * time.Millisecond)
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				continue
			}

			// Check peers commit log or not
			// if commit proportion more than half, leader commit
			for idx := rf.commitIndex + 1; idx <= rf.logsLen(); idx++ {
				if rf.logs[idx].Term != rf.currentTerm {
					continue
				}
				cnt := 1
				for serverId, _ := range rf.peers {
					if serverId == rf.me || rf.matchIndex[serverId] < idx {
						continue
					}
					cnt++
					if cnt > len(rf.peers) / 2 {
						rf.commitIndex = max(rf.commitIndex, idx)
						DPrintf("%s leader update commit index.", rf)
						break
					}
				}
				DPrintf("%s leader check log %v commit num: %v.", rf, idx, cnt)
			}

			rf.mu.Unlock()
		}
	}()

	// Apply msg loop for all server
	go func() {
		for rf.isAlive {
			time.Sleep(time.Duration(20) * time.Millisecond)
			rf.mu.Lock()

			//DPrintf("%s lastlogindex: %d.", rf, rf.lastLog().Index)

			if rf.commitIndex > rf.lastApplied && rf.lastLog().Index > rf.lastApplied {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					Index:       rf.lastApplied,
					Command:     rf.logs[rf.lastApplied].Command,
					UseSnapshot: false,
					Snapshot:    nil,
				}
				DPrintf("%s apply msg on index %d, command: %v.", rf, rf.lastApplied, applyMsg.Command)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}

			rf.mu.Unlock()
		}
	}()



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
