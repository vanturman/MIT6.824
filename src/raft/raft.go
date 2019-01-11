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
	"fmt"
	"sync"
)
import "labrpc"

// import "bytes"
// import "labgob"

const(
	Follower=iota
	Candidate
	Leader
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	currentTerm int
	votedFor int
	log []entries
	//volatile state
	state int
	commitIndex int
	lastApplied int

	//leader
	nextIndex []int
	matchIndex []int

	//channel
	chanAppendEntries chan int
	chanVoteGranted chan int
	chanLeader chan int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm int
	//Entries []entries
	//LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	//PrevIndex int
	//Success bool
}

func (rf *Raft) ClearChange() {
	rf.chanAppendEntries = make(chan int, 1000)
	rf.chanVoteGranted = make(chan int, 1000)
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.ClearChange()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.chanVoteGranted <- 1
			reply.VoteGranted = true
			rf.state = Follower
			rf.voteFor = args.CandidateId
			fmt.Println(rf.me, "voted for ", rf.votedFor)
		} 
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.ClearChange()
		chanAppendEntries <- 1
	}
	reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	term := -1
	isLeader := true

	// Your code here (2B).


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
}

// 2A:candidate server request vote for itself
func (rf *Raft) allRequestVote() {
	fmt.Println(rf.me, " becomes candidate, term is ", rf.currentTerm)
	count := 1
	args := &RequestVoteArgs{rf.currentTerm, rf.me}
	for i := 0; i < len(rf.peers); i++ {
		if rf.state == Candidate && i != rf.me { //only candidate can send requestVote
			go func(i int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok == true && rf.state == Candidate {// must verify rf's identity in case it became a follower
					if reply.VoteGranted == true {	// receive positive vote from rf.peers[i]
						count += 1
						if (count == len(rf.peers) + 1) && (rf.state == Candidate){ //  must verify rf's identity in case it became a follower
							rf.chanLeader <- 1
						}
					} 
					else if reply.Term > rf.currentTerm { //candidate term is out of date
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.ClearChange()
						return
					}
					if args.Term > rf.currentTerm { //candidate timeout and start a new election
						return
					}

				}
			}(i)
		}
	}
}

// 2A & 2B: leader send appendEntries or heartbeats rpc to servers 
func (rf *Raft) allAppendEntries() {
    //rules for leaders to increase the commitIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if rf.state == Leader && i != rf.me {
			args = &AppendEntriesArgs{rf.currentTerm, rf.me}
			go func(args *AppendEntriesArgs, i int) {
				reply = &AppendEntriesApply{}
				ok := rf.peers[i].Call("Raft.AppendEntries", args, apply)
				rf.mu.Lock()
				rf.mu.Unlock()
				if ok == true && rf.state== Leader {
					if args.Term ! =rf.currentTerm {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.ClearChange()
						return
					}

				}
			}(args, i)
		}
	}
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

// 2A: do state change
func (rf *Raft)doStateChange {
	for {
		rf.mu.Lock()
		st := rf.state
		rf.mu.Unlock()
		switch st {
		case Follower:
			select {
			case <- chanAppendEntries: //receive AppendEntries(empty means heartbeat) rpc
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <- chanVoteGranted: //receive VoteRequest from candidate
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <- time.After(time.Millisecond * time.Duration(750 + rand.Int31n(500))): //election timeout
				rf.mu.Lock()
				if len(chanAppendEntries) == 0 && len(chanVoteGranted) == 0{
					rf.currentTerm += 1
					rf.state = Candidate
					rf.votedFor = rf.me
				}
				rf.ClearChange()
				rf.mu.Unlock()
			}
		case Candidate:
			go rf.allRequestVote() //send rpc to all servers
			select {
			case <- time.After(time.Millisecond * (time.Duration(750) + rand.Int31n(500)))
				rf.mu.Lock()
				if len(rf.chanAppendEntries) == 0 && len(rf.chanVoteGranted) == 0 {
					rf.currentTerm += 1
					rf.votedFor = rf.me
				}
				rf.ClearChange()
				rf.mu.Unlock()
			case <- chanLeader:	//receive more than major votes
				rf.mu.Lock()
				rf.state = Leader
				rf.Println(rf.me, "become new leader, term is ", rf.currentTerm)
				rf.mu.Unlock()
			case <- chanAppendEntries:	//receive new leader's rpc and convert to Follower
				rf.mu.Lock()
				rf.state = Follower
				rf.ClearChange()
				rf.mu.Unlock()
			}

		case Leader:
			go rf.allAppendEntries()
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.chanAppendEntries = make(chan int, 1000)
	rf.chanVoteGranted = make(chan int, 1000)
	rf.chanLeader = make(chan int, 1000)


	go rf.doStateChange()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
