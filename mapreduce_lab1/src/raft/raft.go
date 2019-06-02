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
// ApplyMsgs
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "labgob"

const (
	LEADER    = 0
	FOLLOWER  = 1
	CANDIDATE = 2
)

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

// LogEntry
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persist data
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	role int
	// how many votes a candidate has received
	votes int
	// how many votes a candidate should receive to become a leader
	major int

	nextIndex  []int
	matchIndex []int

	lastReceivedTime  time.Time
	electionStartTime time.Time
	sendHeartBeatTime time.Time

	//electionTimeOut       time.Duration
	//receiveMessageTimeOut time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//fmt.Printf("rf persist log \n")

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//  error...
		fmt.Printf("readPersist has error \n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	//fmt.Println("Enter Request Vote")
	//fmt.Printf("requester id %v requester term %v currentraft id %v current raft term %v \n", args.CandidateID, args.Term, rf.me, rf.currentTerm)
	//resetTimer(&rf.lastReceivedTime)
	rf.lastReceivedTime = time.Now()
	//fmt.Printf("%v reset receive requestvote time \n", rf.me)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T,
	// convert to follower
	// when first convert to follower(from candidate), set votedFor = -1
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1

	}

	// if args.Term == rf.currentTerm, still need next step log checking to determine give vote or not
	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = false
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID

		//  fmt.Printf("%v give his vote to %v \n", rf.me, args.CandidateID)
	}
	rf.persist()
	rf.mu.Unlock()

	return
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	// carefully acquire lock AFTER this rpc call
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			rf.votedFor = -1
		} else if reply.VoteGranted == true {
			rf.votes++
			if rf.role == CANDIDATE && rf.votes >= rf.major {
				rf.role = LEADER
				//  fmt.Printf("%v wins the election at time %s \n", rf.me, time.Now())
				// initialize nextIndex[] matchIndex[]
				lastLogIndex1 := rf.log[len(rf.log)-1].Index + 1
				for i := range rf.peers {
					rf.nextIndex[i] = lastLogIndex1
					rf.matchIndex[i] = 0
				}
				go rf.sendHeartbeat()
				//      fmt.Printf("< %v becomes leader at term %v > \n", rf.me, rf.currentTerm)
			}
		}
	}
	rf.persist()
	rf.mu.Unlock()
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesHandler(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	rf.lastReceivedTime = time.Now()
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//reply.NextIndex = rf.getLastIndex() + 1
		//	fmt.Printf("%v currentTerm: %v rejected %v:%v\n",rf.me,rf.currentTerm,args.LeaderId,args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	if args.PrevLogIndex > len(rf.log) {
		//reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	//reply.NextIndex = rf.getLastIndex() + 1

	reply.Term = rf.currentTerm

	rf.persist()
	rf.mu.Unlock()
	return
}

func (rf *Raft) AppendEntriesHandler(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	rf.lastReceivedTime = time.Now()

	if args.Term >= rf.currentTerm {
		if rf.role == CANDIDATE {
			rf.role = FOLLOWER
			rf.votedFor = -1
		}
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.role = FOLLOWER
			rf.votedFor = -1
		}

		// receiving heartbeat
		// receive appendlog command
		// TODO: 3B, 3C receive appendlog command
		//fmt.Printf("%v append log \n", rf.me)
		//fmt.Printf("%v # %v \n", args, rf)
		// if previous log doesn't match, return false instantly
		if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false

		} else {
			// prevLog match
			// remove all items after prevLog
			if len(args.Entries) != 0 {
				rf.log = rf.log[0 : args.PrevLogIndex+1]
				//append all logs in args
				for i := 0; i < len(args.Entries); i++ {
					rf.log = append(rf.log, args.Entries[i])
				}
				//rf.log = append(rf.log, args.Entries...)
			}
			reply.Success = true

			// if len(args.Entries) != 0 {
			//  //fmt.Printf("%v append log %v loglength %v, log is %v \n", rf.me, reply.Success, len(rf.log), rf.log)
			// }
			if args.LeaderCommit > rf.commitIndex {
				lastLogIndex := rf.log[len(rf.log)-1].Index
				if args.LeaderCommit > lastLogIndex {
					rf.commitIndex = lastLogIndex
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				//  fmt.Printf("follower %v update commitedIndex = %v \n", rf.me, rf.commitIndex)
			}
		}

	} else {
		reply.Success = false
	}
	reply.Term = rf.currentTerm

	rf.persist()
	rf.mu.Unlock()
	return
}

// func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

// 	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
// 	rf.mu.Lock()
// 	// if len(args.Entries) != 0 {
// 	//  //  fmt.Printf("%v send %v info args: %v, %v  ok is %v, rf.currentTerm = %v, reply.term = %v \n", rf.me, server, args, reply, ok, rf.currentTerm, reply.Term)
// 	// }

// 	if ok {
// 		if reply.Term > rf.currentTerm {
// 			rf.currentTerm = reply.Term
// 			rf.role = FOLLOWER
// 			rf.votedFor = -1

// 			//fmt.Printf("after receive msg from %v, %v isn't leader anymore \n", server, rf.me)
// 		} else { // not heartbeat return
// 			//TODO 3B 3C appending log
// 			//fmt.Printf("%v send %v reply is: %v \n", rf.me, server, reply.Success)
// 			if reply.Success == true {
// 				if len(args.Entries) != 0 {

// 					// update the nextIndex as last entry's next index
// 					rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
// 					rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index // match index use？
// 				} else {
// 					// rf.nextIndex[server]++
// 					// rf.matchIndex[server]++ // match index use？
// 				}
// 				//  fmt.Printf("%v sends appendlog instr to server %v successfully nextIndex is %v matchIndex is %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
// 			} else {
// 				// log inconsistency, decrement nextIndex and retry
// 				rf.nextIndex[server]--
// 				//  fmt.Printf("leader %v send appendlog to server cause incosistency: %v, rf.nextIndex[server]: %v \n", rf.me, server, rf.nextIndex[server])
// 			}

// 		}
// 	}
// 	rf.persist()

// 	rf.mu.Unlock()
// 	return ok
// }

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
	// what if the instance is killed? how to return gracefully
	rf.mu.Lock()
	isLeader := rf.role == LEADER
	var index int
	var term int
	if isLeader {

		index = rf.log[len(rf.log)-1].Index + 1 // if it's ever committed
		term = rf.currentTerm
		//  fmt.Printf("leader %v start to append command %v at term %v with index %v \n", rf.me, command, rf.currentTerm, index)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command, Index: index})
		rf.persist()

		//term = rf.currentTerm
		go rf.startAgreement()
	}
	// Your code here (3B).
	//return immediately
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) startAgreement() {
	rf.mu.Lock()
	//rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, Index: rf.log[len(rf.log)-1].Index + 1})
	LastLogIndex := rf.log[len(rf.log)-1].Index
	//rf.agreement = 1
	//fmt.Printf("%v starts agreement with log command %v at index %v \n", rf.me, command, LastLogIndex)
	for i := range rf.peers {
		if i != rf.me && rf.role == LEADER {

			if LastLogIndex >= rf.nextIndex[i] {
				entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]:]))
				copy(entries, rf.log[rf.nextIndex[i]:])
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, Entries: entries, LeaderCommit: rf.commitIndex}
				var reply AppendEntriesReply
				go rf.sendAppendEntries(i, args, &reply)
			}

		}
	}

	rf.mu.Unlock()
}
func (rf *Raft) checkCommited() {
	rf.mu.Lock()
	//fmt.Printf("leader %v check commited \n", rf.me)
	for N := len(rf.log) - 1; rf.log[N].Term == rf.currentTerm && N > rf.commitIndex; N-- {
		var counts int
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				counts++
			}
		}
		//from the highest index to lowest index, commit only once a time
		if counts >= rf.major {
			rf.commitIndex = N
			//fmt.Printf("leader %v check commited, update commitedIndex = %v \n", rf.me, rf.commitIndex)
			//  go rf.applyToStateMachine()
			break
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	if rf.lastApplied < rf.commitIndex {

		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		for i := lastApplied + 1; i <= commitIndex; i++ {
			//  fmt.Printf("apply to state machine: %v, i: %v, loglength = %v, log is %v, commitIndex=%v  \n", rf.me, i, len(rf.log), rf.log, commitIndex)
			msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			//  fmt.Printf("%v sends command %v to channel \n", rf.me, rf.log[i].Command)
			rf.applyCh <- msg
			//  m := <-rf.applyCh
			//  fmt.Printf("%v reads command %v from channel \n", rf.me, m)

		}
		rf.lastApplied = rf.commitIndex
		//  fmt.Printf("%v applyto state machine rf.lastApplied = %v, rf.commitIndex=%v \n", rf.me, rf.lastApplied, rf.commitIndex)
	}
	rf.mu.Unlock()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

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
	rf.votedFor = -1
	rf.role = FOLLOWER // start with role as follower
	rf.currentTerm = 0
	rf.applyCh = applyCh

	rf.major = len(rf.peers) / 2

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.lastReceivedTime = time.Now()

	electionTimeOut := time.Duration(time.Millisecond) * time.Duration(240+rand.Intn(100))
	receiveMessageTimeOut := time.Duration(time.Millisecond) * time.Duration(120+rand.Intn(50))

	// true log index starts with index = 1, append a dummy header into log arraylist(using go slice)
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for true {
			time.Sleep(10 * time.Duration(time.Millisecond))
			rf.mu.Lock()

			if rf.role == FOLLOWER {
				if time.Now().Sub(rf.lastReceivedTime) > receiveMessageTimeOut {
					go rf.startElection()

				}
			} else if rf.role == LEADER {
				go rf.checkCommited()
				go rf.startAgreement()
				if time.Now().Sub(rf.sendHeartBeatTime) > 105*time.Millisecond {

					go rf.sendHeartbeat()

				}
			} else if rf.role == CANDIDATE {
				if time.Now().Sub(rf.electionStartTime) > electionTimeOut {
					go rf.startElection()
				}
			}
			go rf.applyToStateMachine()
			rf.mu.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	rf.sendHeartBeatTime = time.Now()
	// TODO: heartbeat still need prevLogIndex, prevLogTerm, leaderCommit but without using

	for i := range rf.peers {
		if i != rf.me && rf.role == LEADER {
			args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, nil, rf.commitIndex}
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)
		}

	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	//  fmt.Printf("%v start election \n", rf.me)
	rf.electionStartTime = time.Now()

	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votes = 0 // reset how many votes
	rf.votedFor = rf.me
	rf.persist()

	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}

	for i := range rf.peers {
		if i != rf.me && rf.role == CANDIDATE {
			var reply RequestVoteReply
			go rf.sendRequestVote(i, args, &reply)
		}
	}

	rf.mu.Unlock()
}
