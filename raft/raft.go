package raft

// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l2cup/raft/labrpc"
)

// import "bytes"
// import "github.com/l2cup/raft/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Follower  State = 0
	Candidate       = 1
	Leader          = 2
)

const (
	HeartbeatTimeout             = 100 * time.Millisecond
	CommitApplyIdleCheckInterval = 25 * time.Millisecond
	IdlePeriodTimeout            = 10 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// Lock to protect shared access to this peer's state.
	mu sync.Mutex

	// Server's current state. Follower, candidate or leader.
	state State

	// RPC endpoints of all peers.
	// MIT's network simulation specific field.
	peers []*labrpc.ClientEnd

	// This peer's index into peers[]
	// MIT's network simulation specific field.
	me int

	// Object to hold this peer's persisted state.
	persister *Persister

	// Latest term server has seen. Initialized to 0, increased monotonically.
	// Persistent on all servers.
	currentTerm int

	// candidateId that recieved the vote in current term.
	// Persistent on all servers.
	votedFor int

	// Log entries. Each entry contains a command for the state machine and term
	// when the entry recieved by leader.
	// First index of log is 1. Because of that every LogEntry has it's index as a field.
	// Persistent on all servers.
	log []LogEntry

	// Index of highest log entry to be commited. Initialized to 0, increases monotonically.
	// Volatile on all servers.
	commitIndex int

	// Index of highest log entry to be applied. Initialized to 0, increases monotonically.
	// Volatile on all servers.
	lastApplied int

	// For each server, index of the next log entry to send to that server.
	// Initialized to leader lastLogIndex + 1
	// Volatile on leaders. Reinitialized after election.
	nextIndex []int

	// For each server, index of the highest log entry known to be replicated on the server.
	// Initialized at 0, increases monotonically.
	// Volatile on leaders. Reinitialized after election.
	matchIndex []int

	// Last hearbeat this server got.
	lastHeartbeat time.Time

	// Leader's Id.
	leaderId int

	// Set by Kill(). Used as a flag, int32 because there is no atomic.SetBoolean().
	dead int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	return term, isLeader
}

func (rf *Raft) toFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

func (rf *Raft) toCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
}

func (rf *Raft) toLeader() {
	rf.votedFor = -1
	rf.state = Leader
	go rf.sendAppendEntries()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// // restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArguments, reply *RequestVoteReply) {
	rf.mu.Lock()
	lastEntry := LogEntry{
		Term:  0,
		Index: 0,
	}

	if len(rf.log) > 0 {
		lastEntry = rf.log[len(rf.log)-1]
	}
	didNotVote := rf.votedFor == -1
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	upToDateLog := func() bool {
		if lastEntry.Term == args.LastLogTerm {
			return args.LastLogIndex >= lastEntry.Index
		}
		return args.LastLogTerm > lastEntry.Term
	}

	reply.Term = currentTerm

	if args.Term < currentTerm {
		reply.VoteGranted = false
	}

	if didNotVote && upToDateLog() {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArguments, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Got heartbeat with entries len %d from [%d].", rf.me, len(args.Entries), args.LeaderId)

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	if args.Term >= rf.currentTerm {
		rf.leaderId = args.LeaderId
		rf.toFollower(args.Term)
	}

	if rf.leaderId == args.LeaderId {
		rf.lastHeartbeat = time.Now()
	}

	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	if len(rf.log) < args.PrevLogIndex {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
		rf.log = rf.log[:args.PrevLogIndex-1]
		return
	}

	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	return
}

func (rf *Raft) sendAppendEntries() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		prevLogIndex := 0
		prevLogTerm := 0
		commitIndex := rf.commitIndex
		if len(rf.log) > 1 {
			prevEntry := rf.log[len(rf.log)-2]
			prevLogTerm = prevEntry.Term
			prevLogIndex = prevEntry.Index
		}
		leaderId := rf.me
		rf.mu.Unlock()

		args := &AppendEntriesArguments{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: commitIndex,
		}
		replies := make([]AppendEntriesReply, len(rf.peers))
		for i, peer := range rf.peers {
			if i == leaderId {
				continue
			}
			peer.Call("Raft.AppendEntries", args, &replies[i])
		}
		time.Sleep(HeartbeatTimeout)
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArguments, reply *RequestVoteReply, voteChan chan int) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		voteChan <- server
	}
}

func (rf *Raft) timeout() {
	timeoutLength := (200 + time.Duration(rand.Intn(300))) * time.Millisecond
	now := <-time.After(timeoutLength)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.killed() {
		if rf.state != Leader && now.Sub(rf.lastHeartbeat) >= timeoutLength {
			DPrintf("[%d] Timer timed out. Starting election.\n", rf.me)
			go rf.startElection()
		}
		go rf.timeout()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.toCandidate()

	// This is here if it's the first vote without logs.
	lastLogTerm := 0
	lastLogIndex := 0

	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	args := &RequestVoteArguments{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	voteChan := make(chan int)
	replies := make([]RequestVoteReply, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, &replies[i], voteChan)
	}

	votes := 1
	for i := 0; i < len(replies)-1; i++ {
		reply := replies[<-voteChan]

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			DPrintf("[%d] Converting to follower as we got a reply with a term %d\n", rf.me, reply.Term)
			rf.toFollower(reply.Term)
			rf.mu.Unlock()
			break
		}
		if reply.VoteGranted == true {
			votes += 1
		}
		if votes > len(replies)/2 {
			if rf.state == Candidate && rf.currentTerm == args.Term {
				DPrintf("[%d] Won the election for term %d.\n", rf.me, args.Term)
				rf.toLeader()
			} else {
				DPrintf("[%d] Election for term %d failed.\n", rf.me, args.Term)
			}
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term, isLeader := rf.GetState()
	if isLeader == false {
		return index, term, isLeader
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
		state:       Follower,
		peers:       peers,
		me:          me,
		persister:   persister,
		currentTerm: 0,
		votedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		dead:        0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}

	for index := range peers {
		if index == me {
			continue
		}
		rf.nextIndex[index] = 1
		rf.matchIndex[index] = 0
	}

	go rf.timeout()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf

}
