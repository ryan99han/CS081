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
	"log"
	"time"
	"sync"
	"math/rand"	
	"sync/atomic"
	"cs90/ds/labrpc"
)

type Role int
// import "bytes"
// import "../labgob"
const (
	Leader    	Role = 0
	Candidate 	Role = 1
	Follower  	Role = 2
)

const (
	TimeoutLength = 400
	ElectionTimeout = time.Millisecond * TimeoutLength
	Heartbeat = time.Millisecond * 200
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role 	      Role
	term 	      int
	votedFor      int
	lastEntryTime time.Time    
	applyChannel  chan ApplyMsg 

	log           []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int 
	matchIndex    []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.role == Leader
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


//
// restore previously persisted state.
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

type LogEntry struct {
	Term 		 int
	Index 		 int
	Command 	 interface{}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted		bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int 
	Entries      []LogEntry
}

type AppendEntryReply struct {
	Term 	     int
	Success		 bool
	NextStartIndex int
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) isUpToDate(index int, term int) bool {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	if lastLogTerm > term || (lastLogTerm == term && lastLogIndex > index) {
		return false
	}
	return true
}

func (rf *Raft) validRequestState(
	correctRole Role, 
	currTerm int, 
	argsTerm int, 
	replyTerm int,
) bool {
	if rf.role != correctRole || rf.term != argsTerm {
		return false
	}
	if replyTerm > currTerm {
		rf.term = replyTerm
		rf.role = Follower
		rf.votedFor = -1
		return false
	}
	return true
}

func (rf *Raft) validResponseState(argsTerm int) bool {
	if argsTerm < rf.term {
		return false
	}
	if argsTerm > rf.term {
		rf.term = argsTerm
		rf.role = Follower
		rf.votedFor = -1
	}
	return true
}	

func (rf *Raft) applyLog() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = i
		msg.Command = rf.log[i].Command
		rf.applyChannel <- msg
	}
	rf.lastApplied = rf.commitIndex
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == Leader	
	
	if isLeader && !rf.killed() {
		index = rf.getLastLogIndex() + 1
		term = rf.term 

		entry := LogEntry{
			Term:    rf.term,
			Index:     index,
			Command: command,	
		}
		rf.log = append(rf.log, entry)
	}
	return index, term, isLeader
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
func (rf *Raft) sendRequestVote() {
	rf.mu.Lock()
	rf.term += 1
	rf.role = Candidate 
	rf.votedFor = rf.me
	
	term := rf.term
	votes := 1
	done := false

	log.Printf("[%d] attempting an election at term %d", rf.me, rf.term)
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, term int) {
			// log.Printf("[%d] sending request vote to %d", rf.me, server)
			rf.mu.Lock()
			args := RequestVoteArgs {
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm: rf.getLastLogTerm(),
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.validRequestState(Candidate, term, args.Term, reply.Term) {
				return
			}

			votes += 1
			// log.Printf("[%d] got vote from %d for term %d", rf.me, server, term)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true

			rf.role = Leader
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = rf.getLastLogIndex() + 1
			}
			
			log.Printf("[%d] we got enough votes, we are now the leader (term = %d)", rf.me, rf.term)
		} (server, term)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.term
	reply.VoteGranted = false

	if !rf.validResponseState(args.Term) {
		return
	}
	// if args.Term == rf.term { // Check if ever hits
	// 	if rf.role == Leader {
	// 		return
	// 	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	// 		return
	// 	}
	// }

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.role = Follower
			rf.term = args.Term
			rf.votedFor = args.CandidateId	
		}
	}
	
	return
}

func (rf *Raft) sendAppendEntries() {
	// log.Printf("[%d] sending append entries at term %d", rf.me, rf.term)

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.mu.Lock()
		term := rf.term
		leader := rf.me
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		commitIndex := rf.commitIndex

		entries := make([]LogEntry, len(rf.log))
		copy(entries, rf.log)
		rf.mu.Unlock()

		go func(server int, 
			    term int, 
			    leader int, 
			    prevLogIndex int, 
			    prevLogTerm int, 
			    commitIndex int,
			    entries []LogEntry) {

			args := AppendEntryArgs {
				Term: term,
				LeaderId: leader,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				LeaderCommit: commitIndex,
				Entries: entries,
			}
			var reply AppendEntryReply

			ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
			if !ok {
				return
			}
			// log.Printf("[%d] finished sending append entry to %d", rf.me, server)


			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !rf.validRequestState(Leader, term, args.Term, reply.Term) {
				return
			}

			if !reply.Success {
				rf.nextIndex[server] = min(reply.NextStartIndex, rf.getLastLogIndex())
			} else {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}


			// oldCommitIndex := rf.commitIndex

			logIndex := rf.getLastLogIndex()
			for {
				if logIndex <= rf.commitIndex || rf.log[logIndex].Term != rf.term {
					break
				}
				total := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= logIndex {
						total++
					}
				}
				if total > len(rf.peers)/2 {
					rf.commitIndex = logIndex
					rf.applyLog()
					break
				}
				logIndex -= 1
			}
		} (server, term, leader, prevLogIndex, prevLogTerm, commitIndex, entries)
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastEntryTime = time.Now()
	rf.role = Follower

	reply.Term = rf.term
	reply.Success = false
	reply.NextStartIndex = rf.getLastLogIndex() + 1

	if !rf.validResponseState(args.Term) {
		return
	}
	
	// Previous log does not exist in raft's log
	if args.PrevLogIndex > rf.getLastLogIndex() {
		return
	}
	// Previous log is not the same in raft's log 
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextStartIndex = i + 1
				return
			}
		}
	}
	// Delete entries which exist in current log's index
	index := args.PrevLogIndex + 1
	if index < len(rf.log) && rf.log[index].Term != args.Term {
		rf.log = rf.log[:index]
	}

	// Append entries to raft's logs
	for _, entry := range args.Entries[index:] {
		rf.log = append(rf.log, entry)
	}
	// rf.log = append(rf.log, args.Entries[index:])
	reply.Success = true
	reply.NextStartIndex = len(args.Entries)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyLog()
	}

	return
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.term = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.lastEntryTime = time.Now()
	rf.applyChannel = applyCh

	rf.log = make([]LogEntry, 0)
	entry := LogEntry {
		Term:    0,
		Index:   0,
		Command: nil,
	}
	rf.log = append(rf.log, entry)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			if rf.killed() {
				return
			}

			if _, isLeader := rf.GetState(); isLeader == true {
				continue
			}

			rf.mu.Lock()
			t := time.Now()
			elapsed := t.Sub(rf.lastEntryTime)
			rf.mu.Unlock()

			if elapsed > time.Duration(ElectionTimeout) {
				rf.sendRequestVote()
			}
			time.Sleep(ElectionTimeout + time.Duration(rand.Int63()) % ElectionTimeout)
		}
	}()

	go func() {
		for {
			if rf.killed() {
				return
			}

			if _, isLeader := rf.GetState(); isLeader == true {
				rf.sendAppendEntries()
				time.Sleep(Heartbeat)
			}
		}
		
	}()
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
