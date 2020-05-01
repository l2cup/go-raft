package raft

// LogEntry type.
// Every log entry has the term when it was commited, the command
// which the state machine has to execute, and it's index.
// The index is in the type because the indexing starts at 0 so we keep the index here
// for simplicity.
type LogEntry struct {
	// The log entry index.
	Index int

	// Current term of the log entry.
	Term int

	// The command which the state machine has to execute.
	Command interface{}
}

// AppendEntries RPC arguments type.
type AppendEntriesArguments struct {
	// Leader's current term.
	Term int

	// Leader's id so the followers can redirect clients.
	LeaderId int

	// Leader of log entry immediately preceding the new entries.
	PrevLogIndex int

	// Term of PrevLogIndex entry.
	PrevLogTerm int

	// New log entries to store.
	// This is empty if it's a heartbeat, may be more than one for efficiency.
	Entries []LogEntry

	// Leader's commit index.
	LeaderCommit int
}

// AppendEntries RPC reply type.
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself.
	Term int
	// True if follower contained entry matching PrevLogIndex and PrevLogTerm
	Success bool

	// ConflictTerm is the term of the conflicting entry in the log.
	// -1 if there is not a conflicting entry.
	ConflictTerm int

	// ConflictIndex is the index of the conflicting entry in the log.
	// -1 if there is not a conflicting entry.
	ConflictIndex int
}

// RequestVote RPC arguments type.
type RequestVoteArguments struct {
	// Candidate's term.
	Term int

	// Candidate's id.
	CandidateId int

	// Index of candidate's last log entry.
	LastLogIndex int

	// Term of candidate's last log entry.
	LastLogTerm int
}

// RequestVote RPC reply type.
type RequestVoteReply struct {
	// currentTerm for candidate to update itself.
	Term int

	// True if candidate recieved the vote, otherwise false.
	VoteGranted bool
}

// TODO: InstallSnapshotRPC arguments and reply.
