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
	"sync"
	"labrpc"
	"math"
	"math/rand"
	"time"
	//"bytes"
	"strconv"
)

// import "labgob"

const Leader = "Leader"
const Follower = "Follower"
const Candidate = "Candidate"
const HeartbeatInterval = 120
const ElectionTimeOutCheckInterval = 30 


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

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex // Lock to protect shared access to this peer's state
	newCond *sync.Cond // signals when Register() adds to workers[]
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	lastUpdateTime time.Time // the last time at which the peer heard from the leader
	electionTimeOut int // ms
	state string
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in currentterm (or -1 if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	//Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := 0
	isleader := false
	// Your code here (2A).
	rf.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	rf.Unlock()
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
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote, index
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	// update term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // reset votefor
		DPrintf("%s become follower RequestVote update term", strconv.Itoa(rf.me))
	}
	// rpc handle
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm {
		if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
			index := len(rf.log) - 1
			if args.LastLogIndex >= index {
				rf.votedFor = args.CandidateId
				DPrintf("%s vote for %s", strconv.Itoa(rf.me), strconv.Itoa(args.CandidateId))
				rf.state = Follower
				DPrintf("%s become follower RequestVote", strconv.Itoa(rf.me))
				reply.VoteGranted = true
			}
		}
	}
	reply.Term = rf.currentTerm
	// update timestamp
	rf.lastUpdateTime = time.Now()
	rf.Unlock()
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

//append Entries
type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	// update term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // reset votefor
		DPrintf("%s become follower AppendEntries update term", strconv.Itoa(rf.me))
	}
	// rpc handle
	reply.Success = false
	logsSize := int(math.Max(float64(len(rf.log) - 1), 0))
	if args.Term >= rf.currentTerm &&  logsSize >= args.PrevLogIndex {
		DPrintf("%s copylogs pre", strconv.Itoa(rf.me))
		if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			// copy logs
			DPrintf("%s copylogs", strconv.Itoa(rf.me))
			copyIndex := 0 // index where we want to copy
			for i, logEntry := range args.Entries {
				copyIndex = i
				index := i + 1 + args.PrevLogIndex
				if logsSize >= index {
					if rf.log[index].Term != logEntry.Term {
						rf.log = rf.log[0 : index] // the Term is not equal
						break
					}
				} else {
					break // meet the end
				}
			}
			for ; copyIndex < len(args.Entries); copyIndex++ {
				rf.log = append(rf.log, args.Entries[copyIndex])
			}
			// update commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log) - 1)));
				if rf.commitIndex > rf.lastApplied {
					rf.newCond.Broadcast()
				}
			}
			rf.state = Follower
			DPrintf("%s become follower AppendEntries", strconv.Itoa(rf.me))
			reply.Success = true
		}	
	}
	reply.Term = rf.currentTerm
	//update timestamp
	rf.lastUpdateTime = time.Now()
	rf.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

// heatbeat function
func (rf *Raft) heartbeat() {
	for {
		rf.Lock()
		if rf.state == Leader {
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.sendAppendEntries(i, args, new(AppendEntriesReply))
				}
			}
		}
		rf.Unlock()
		time.Sleep(HeartbeatInterval * time.Millisecond)
	}
}

func (rf *Raft) elect() {
	rf.Lock()
	duration := int(time.Now().Sub(rf.lastUpdateTime).Nanoseconds() / 1e6)
	ch := make(chan RequestVoteReply, 1)
	if (rf.state == Follower || rf.state == Candidate) && duration >= rf.electionTimeOut {
		DPrintf("### %s duration:%s timeout:%s", strconv.Itoa(rf.me), strconv.Itoa(duration), strconv.Itoa(rf.electionTimeOut))
		DPrintf("### %s start election %s ###", strconv.Itoa(rf.me), rf.state)
		// start election
		rf.currentTerm += 1
		rf.state = Candidate
		rf.votedFor = rf.me
		args := &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0} 
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(index int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(index, args, &reply)
					if ok {
						ch <- reply
					}
				}(i)
			}
		}
		rf.lastUpdateTime = time.Now()
	}
	currentTerm := rf.currentTerm
	rf.Unlock()
	// wait select
	votesGet := 1
	isTimeout := false
	voteSuceess := false
	// timeout routine
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(rf.electionTimeOut) * time.Millisecond)
		timeoutCh <- true
	}()

	for {
		select {
			case rsp := <-ch:
				rf.Lock()
				if rf.state == Candidate && rsp.VoteGranted && rsp.Term == rf.currentTerm {
					DPrintf("get vote %s", strconv.Itoa(rf.me))
					votesGet += 1
				}
				rf.Unlock()
			case <-timeoutCh:
				isTimeout = true
		}
		if isTimeout {
			break
		} else if votesGet > len(rf.peers) / 2 {
			DPrintf("get vote > semi %s", strconv.Itoa(rf.me))
			voteSuceess = true
			break
		}
	}
	rf.Lock()
	if rf.state == Candidate && voteSuceess && currentTerm == rf.currentTerm {
		DPrintf("become leader %s", strconv.Itoa(rf.me))
		rf.state = Leader
	}
	rf.Unlock()
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
	// init
	rf.state = Follower //init from follower
	rf.newCond = sync.NewCond(rf)
	rf.currentTerm = 0
	rf.votedFor = -1 // no voted for any raft-server
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.matchIndex[i] = 0
	}
	// single routine for emit msg to applyCh 
	go func(rf *Raft, applyCh chan ApplyMsg) {
		for {
			rf.Lock()
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++;
				applyCh <- ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
			} else {
				rf.newCond.Wait()
			}
			rf.Unlock()
		}
	}(rf, applyCh)
	// single routine for heartbeat
	go rf.heartbeat()
	// single routine for election
	rf.lastUpdateTime = time.Now() // get currentTime
	rand.Seed(rf.lastUpdateTime.UnixNano())
	rf.electionTimeOut = 400 + rand.Intn(201)
	go func() {
		for {
			time.Sleep(time.Millisecond)
			rf.Lock()
			duration := int(time.Now().Sub(rf.lastUpdateTime).Nanoseconds() / 1e6)
			if (rf.state == Follower || rf.state == Candidate) && duration >= rf.electionTimeOut {
				go rf.elect()
			}
			rf.Unlock()
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}