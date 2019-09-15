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
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "labgob"

const Leader = "Leader"
const Stopped = "Stopped"
const Follower = "Follower"
const Candidate = "Candidate"
const PreCandidate = "PreCandidate"
const HeartbeatInterval = 180
const ElectionTimeOut= 400


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

type event struct {
	req interface{}
	resp interface{}
	errChan chan error
	index int // peer(receiver) instance index
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	rwMutex sync.RWMutex // Lock to protect shared access to this peer's state
	routineGroup sync.WaitGroup
	stopped chan bool
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state string
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	coreEventChan chan *event // eventloop consume event from this chan
	networkEventChan chan *event // network routine consume event from this chan
	applyChan chan ApplyMsg // user fsm chan

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
func (r *Raft) GetState() (int, bool) {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	return r.currentTerm, r.state == Leader
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
	PreVote bool // is preVote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
	PreVote bool // is preVote
}

//
// example RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	r.send(args, reply);
}


func (r *Raft) processRequestVoteRequest(req *RequestVoteArgs, resp *RequestVoteReply) bool {
	// Your code here (2A, 2B).
	resp.PreVote = req.PreVote
	if req.Term < r.currentTerm {
		resp.Term = r.currentTerm
		resp.VoteGranted = false;
		return false;
	} else if req.Term == r.currentTerm && r.votedFor != -1 && r.votedFor != req.CandidateId {
		resp.Term = r.currentTerm
		resp.VoteGranted = false
		return false
	}

	if req.PreVote == false {
		// ignore preVote
		if req.Term > r.currentTerm {
			r.updateCurrentTerm(req.Term)
		}
	}

	// check log

	if len(r.log) == 0 {
		if req.LastLogIndex != 0 {
			resp.Term = r.currentTerm
			resp.VoteGranted = false
			return false
		}
	} else {
		if r.log[len(r.log) - 1].Term > req.LastLogTerm ||
			(r.log[len(r.log) - 1].Term == req.LastLogTerm &&
				len(r.log) > req.LastLogIndex) {
			resp.Term = r.currentTerm
			resp.VoteGranted = false
			return false
		}
	}

	DPrintf(r.me, r.currentTerm, r.state, "vote for ", req.CandidateId)
	if req.PreVote == false {
		// ignore preVote
		r.votedFor = req.CandidateId
	}
	resp.Term = r.currentTerm
	resp.VoteGranted = true
	return true
}

func (r *Raft) updateCurrentTerm(term int) {
	if term < r.currentTerm {
		panic("updateCurrentTerm panic");
	}
	r.rwMutex.Lock();
	defer r.rwMutex.Unlock();
	r.currentTerm = term;
	r.state = Follower;
	DPrintf(r.me, r.currentTerm, r.state,"become follower")
}

func (r *Raft) processRequestVoteResponse(resp *RequestVoteReply, preVote bool) bool  {
	if resp.PreVote != preVote {
		return false
	}
	if resp.Term < r.currentTerm {
		return false
	}
	if resp.Term == r.currentTerm && resp.VoteGranted {
		return true
	}
	DPrintf(r.me, r.currentTerm, r.state,"request vote failed")
	if resp.Term > r.currentTerm {
		r.updateCurrentTerm(resp.Term)
	}
	return false
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
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
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
	FollowerId int
}

type AppendEntriesReply struct {
	Term int //currentTerm, for leader to update itself
	PrevLogTerm int
	PrevLogIndex int
	EntriesCount int
	FollowerId int
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	Inconsistency bool // true is log not consistent
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.send(args, reply)
}

func (r *Raft) processAppendEntriesRequest(req *AppendEntriesArgs, resp *AppendEntriesReply) bool {
	resp.FollowerId = req.FollowerId
	resp.Inconsistency = false
	resp.PrevLogIndex = req.PrevLogIndex
	resp.PrevLogTerm = req.PrevLogTerm
	if req.Term < r.currentTerm {
		resp.Term = r.currentTerm
		resp.Success = false;
		return false
	}
	if req.Term == r.currentTerm {
		if r.state == Candidate || r.state == PreCandidate {
			r.rwMutex.Lock()
			r.state = Follower
			r.rwMutex.Unlock()
			DPrintf(r.me, r.currentTerm, r.state,"become follower")
		}
	} else {
		r.updateCurrentTerm(req.Term)
	}
	// log implements

	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()

	// check commitIndex
	if req.PrevLogIndex < r.commitIndex {
		resp.Success = false
		resp.Term = r.currentTerm
		return true
	}

	// not match
	if req.PrevLogIndex != 0 && (len(r.log) < req.PrevLogIndex ||
		r.log[req.PrevLogIndex - 1].Term != req.PrevLogTerm) {
		resp.Success = false
		resp.Term = r.currentTerm
		resp.Inconsistency = true
		return true
	}

	// truncate
	if req.PrevLogIndex != 0 && len(r.log) > req.PrevLogIndex {
		r.log = r.log[0: req.PrevLogIndex]
	}

	// append entries
	r.log = append(r.log, req.Entries...)

	if req.LeaderCommit > r.commitIndex && len(r.log) > r.commitIndex {
		if req.LeaderCommit <= len(r.log) {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = len(r.log)
		}
	}

	for ;r.lastApplied < r.commitIndex; {
		r.lastApplied ++
		//DPrintf(r.me, r.currentTerm, r.state, "apply index: " ,
		//	r.lastApplied ," command: ", r.log[r.lastApplied - 1].Command)
		r.applyChan <- ApplyMsg{
			CommandValid:true,
			Command:r.log[r.lastApplied - 1].Command,
			CommandIndex: r.lastApplied,
		}
	}


	resp.Term = r.currentTerm
	resp.Success = true
	resp.EntriesCount = len(req.Entries)
	return true
}

func (r *Raft) processAppendEntriesResponse(resp *AppendEntriesReply) {
	if resp.Term < r.currentTerm {
		return
	}
	if resp.Term > r.currentTerm {
		r.updateCurrentTerm(resp.Term)
	}

	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	if !resp.Success {
		if resp.Inconsistency && r.nextIndex[resp.FollowerId] > 1 {
			if resp.PrevLogIndex < r.nextIndex[resp.FollowerId] {
				r.nextIndex[resp.FollowerId] = resp.PrevLogIndex
			}

		}
		return
	}
	// log implements
	if r.nextIndex[resp.FollowerId] < resp.PrevLogIndex + resp.EntriesCount + 1 {
		r.nextIndex[resp.FollowerId] = resp.PrevLogIndex + resp.EntriesCount + 1
		r.matchIndex[resp.FollowerId] = r.nextIndex[resp.FollowerId] - 1
	}

	sortedMatchIndexArray := make([]int, len(r.peers))
	for i, _ := range sortedMatchIndexArray {
		if i == r.me {
			sortedMatchIndexArray[i] = len(r.log)
		} else {
			sortedMatchIndexArray[i] = r.matchIndex[i]
		}
	}
	sort.Ints(sortedMatchIndexArray)
	quorumMatchIndex := sortedMatchIndexArray[len(r.peers) / 2]

	if  quorumMatchIndex > r.commitIndex && r.log[quorumMatchIndex - 1].Term == r.currentTerm {
		r.commitIndex = quorumMatchIndex
	}
	for ;r.lastApplied < r.commitIndex; {
		r.lastApplied ++
		DPrintf(r.me, r.currentTerm, r.state, "apply index: " ,
			r.lastApplied ," command: ", r.log[r.lastApplied - 1].Command)
		r.applyChan <- ApplyMsg{
			CommandValid:true,
			Command:r.log[r.lastApplied - 1].Command,
			CommandIndex: r.lastApplied,
		}
	}
}


func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type CommandRequest struct {
	Command interface{}
}

type CommandReply struct {
	Index int
	Term int
}

func (r *Raft) processCommand(req *CommandRequest, reply *CommandReply) {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	r.log = append(r.log, LogEntry{Command:req.Command, Term:r.currentTerm})
	DPrintf(r.me, r.currentTerm, r.state, "add log index: " ,len(r.log), "command: ", req.Command)
	reply.Index = len(r.log)
	reply.Term = r.currentTerm
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
func (r *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	req := &CommandRequest{Command:command}
	reply := &CommandReply{}
	if err := r.send(req, reply); err != nil {
		return 0, 0, false
	}
	return reply.Index, reply.Term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (r *Raft) Kill() {
	// Your code here, if desired.
	close(r.stopped)
	r.routineGroup.Wait()
	DShortPrintf(r.me, "clean up finish")
}

func (r *Raft) eventLoop() {
	for r.state != Stopped {
		switch r.state {
		case Leader:
			r.leaderLoop()
		case Follower:
			r.followerLoop()
		case Candidate:
			r.candidateLoop()
		case PreCandidate:
			r.preCandidateLoop()
		}
	}
	DShortPrintf(r.me, "eventLoop exit")
}

func (r *Raft) heartbeatToAll() {
	for i := 0; i < len(r.peers); i++ {
		if i != r.me {
			args := &AppendEntriesArgs{}
			args.Term = r.currentTerm
			args.LeaderId = r.me
			args.PrevLogIndex = r.nextIndex[i] - 1
			if args.PrevLogIndex == 0 {
				args.PrevLogTerm = 0
			} else {
				args.PrevLogTerm = r.log[args.PrevLogIndex - 1].Term
			}
			args.LeaderCommit = r.commitIndex
			if len(r.log) >= r.nextIndex[i] {
				args.Entries = r.log[r.nextIndex[i] - 1:len(r.log)]
			} else {
				args.Entries = make([]LogEntry, 0)
			}
			//DPrintf(r.me, r.currentTerm, r.state,"heatbeat to ",  i, " entries:", len(args.Entries))
			args.FollowerId = i
			heartbeatEvent := &event{
				req: args,
				index: i}
			select {
			case r.networkEventChan <- heartbeatEvent:
			case <-r.stopped:
				return
			}
		}
	}
}

func (r *Raft) leaderLoop() {
	// reset index
	r.resetIndex()
	// first send heartbeat to followers
	r.heartbeatToAll()

	heartbeatTicker := time.Tick(HeartbeatInterval * time.Millisecond)
	for r.state == Leader {
		select {
		case <-r.stopped:
			r.state = Stopped
			return
		case e := <-r.coreEventChan:
			switch req := e.req.(type) {
			case *CommandRequest:
				r.processCommand(req, e.resp.(*CommandReply))
				e.errChan <- nil
			case *AppendEntriesArgs:
				_ = r.processAppendEntriesRequest(req, e.resp.(*AppendEntriesReply))
				e.errChan <- nil
			case *RequestVoteArgs:
				_ = r.processRequestVoteRequest(req, e.resp.(*RequestVoteReply))
				e.errChan <- nil
			case *AppendEntriesReply:
				r.processAppendEntriesResponse(req)
			default:
			}
		case <-heartbeatTicker:
			r.heartbeatToAll()
		}
	}
}

func (r *Raft) followerLoop() {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	timeoutChan := time.After(time.Duration(ElectionTimeOut + rand.Intn(201)) * time.Millisecond)
	for r.state == Follower {
		updateTimeout := false
		select {
		case <-r.stopped:
			r.state = Stopped
			return
		case e := <- r.coreEventChan:
			switch req := e.req.(type) {
			case *CommandRequest:
				e.errChan <- fmt.Errorf("not leader")
			case *AppendEntriesArgs:
				updateTimeout = r.processAppendEntriesRequest(req, e.resp.(*AppendEntriesReply))
				e.errChan <- nil
			case *RequestVoteArgs:
				updateTimeout = r.processRequestVoteRequest(req, e.resp.(*RequestVoteReply))
				e.errChan <- nil
			}
		case <-timeoutChan:
			r.rwMutex.Lock()
			r.state = PreCandidate
			r.rwMutex.Unlock()
			DPrintf(r.me, r.currentTerm, r.state,"become preCandidate because of timeout")
			return
		}
		if updateTimeout {
			timeoutChan = time.After(time.Duration(ElectionTimeOut + rand.Intn(201)) * time.Millisecond)
		}
	}
}

func (r *Raft) candidateLoop() {
	needNewVote := true
	votesGranted := 0
	r.votedFor = -1
	var timeoutChan <-chan time.Time
	for r.state == Candidate {
		if needNewVote {
			r.rwMutex.Lock()
			r.votedFor = r.me
			r.currentTerm++
			r.rwMutex.Unlock()
			votesGranted = 1
			r.votedFor = -1
			for i := 0; i < len(r.peers); i++ {
				if i != r.me {
					args := &RequestVoteArgs{
						Term:        r.currentTerm,
						CandidateId: r.me}
					args.LastLogIndex = 0
					args.LastLogTerm = 0
					args.PreVote = false
					if len(r.log) > 0 {
						args.LastLogIndex = len(r.log)
						args.LastLogTerm = r.log[args.LastLogIndex - 1].Term
					}
					requestVoteEvent := &event{req: args, index: i}
					DPrintf(r.me, r.currentTerm, r.state, "request vote from ", i)
					select {
					case r.networkEventChan <- requestVoteEvent:
					case <-r.stopped:
						return
					}
				}
			}
			timeoutChan = time.After(time.Duration(ElectionTimeOut + rand.Intn(201)) * time.Millisecond)
			needNewVote = false
		}
		if votesGranted >= len(r.peers) / 2 + 1 {
			r.rwMutex.Lock()
			r.state = Leader
			r.rwMutex.Unlock()
			DPrintf(r.me, r.currentTerm, r.state,"become leader because of quorum")
			return
		}
		select {
		case <-r.stopped:
			r.state = Stopped
			return
		case e := <- r.coreEventChan:
			switch req := e.req.(type) {
			case *CommandRequest:
				e.errChan <- fmt.Errorf("not leader")
			case *AppendEntriesArgs:
				_ = r.processAppendEntriesRequest(req, e.resp.(*AppendEntriesReply))
				e.errChan <- nil
			case *RequestVoteArgs:
				_ = r.processRequestVoteRequest(req, e.resp.(*RequestVoteReply))
				e.errChan <- nil
			case *RequestVoteReply:
				voteRet := r.processRequestVoteResponse(req, false)
				if voteRet {
					votesGranted++
				}
			}
		case <- timeoutChan:
			needNewVote = true
		}
	}
}

func (r *Raft) preCandidateLoop() {
	needNewPreVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	for r.state == PreCandidate {
		if needNewPreVote {
			r.rwMutex.Lock()
			r.votedFor = r.me
			r.rwMutex.Unlock()
			votesGranted = 1
			for i := 0; i < len(r.peers); i++ {
				if i != r.me {
					args := &RequestVoteArgs{
						Term:        r.currentTerm + 1,
						CandidateId: r.me}
					args.LastLogIndex = 0
					args.LastLogTerm = 0
					args.PreVote = true
					if len(r.log) > 0 {
						args.LastLogIndex = len(r.log)
						args.LastLogTerm = r.log[args.LastLogIndex - 1].Term
					}
					requestVoteEvent := &event{req: args, index: i}
					DPrintf(r.me, r.currentTerm, r.state, "request vote from ", i)
					select {
					case r.networkEventChan <- requestVoteEvent:
					case <-r.stopped:
						return
					}
				}
			}
			timeoutChan = time.After(time.Duration(ElectionTimeOut + rand.Intn(201)) * time.Millisecond)
			needNewPreVote = false
		}
		if votesGranted >= len(r.peers) / 2 + 1 {
			r.rwMutex.Lock()
			r.state = Candidate
			r.rwMutex.Unlock()
			DPrintf(r.me, r.currentTerm, r.state,"become candidate because of quorum")
			return
		}
		select {
		case <-r.stopped:
			r.state = Stopped
			return
		case e := <- r.coreEventChan:
			switch req := e.req.(type) {
			case *CommandRequest:
				e.errChan <- fmt.Errorf("not leader")
			case *AppendEntriesArgs:
				_ = r.processAppendEntriesRequest(req, e.resp.(*AppendEntriesReply))
				e.errChan <- nil
			case *RequestVoteArgs:
				_ = r.processRequestVoteRequest(req, e.resp.(*RequestVoteReply))
				e.errChan <- nil
			case *RequestVoteReply:
				voteRet := r.processRequestVoteResponse(req, true)
				if voteRet {
					votesGranted++
				}
			}
		case <- timeoutChan:
			needNewPreVote = true
		}
	}
}

func (r *Raft) networkWorkerLoop() {
	for {
		select {
		case <-r.stopped:
			DShortPrintf(r.me, "network routine exit")
			return
		case e := <-r.networkEventChan:
			switch req := e.req.(type) {
			case *AppendEntriesArgs:
				go func () {
					resp := &AppendEntriesReply{}
					ok := r.sendAppendEntries(e.index, req, resp)
					if ok {
						r.sendAsync(resp)
					} else {
						DShortPrintf(r.me, "send appendEnties failed to ", e.index)
					}
				} ()
			case *RequestVoteArgs:
				go func () {
					resp := &RequestVoteReply{}
					ok := r.sendRequestVote(e.index, req, resp)
					if ok {
						r.sendAsync(resp)
					} else {
						DShortPrintf(r.me, "send requestVote failed to ", e.index)
					}
				} ()
			}
		}
	}
}

// send async to eventLoop

func (r *Raft) sendAsync(req interface{}) {
	event := &event{req: req, errChan: make(chan error, 1)}
	//DShortPrintf(r.me, "send Async")
	select {
	case r.coreEventChan <- event:
	case <-	r.stopped:
	default:
	}
	r.routineGroup.Add(1)
	go func() {
		defer r.routineGroup.Done()
		select {
		case r.coreEventChan <- event:
		case <- r.stopped:
			DShortPrintf(r.me,"send Async routine exit")
			return
		}
	}()
}

func (r *Raft) send(req interface{}, resp interface{}) error {
	event := &event{req: req, resp: resp, errChan: make(chan error, 1)}
	//DShortPrintf(r.me, "send")
	select {
	case r.coreEventChan <- event:
	case <-r.stopped:
		return nil
	}
	select {
	case err := <- event.errChan:
		return err
	case <- r.stopped:
		return nil
	}
}

func (r *Raft) resetIndex() {
	for i := 0; i < len(r.peers); i++ {
		r.matchIndex[i] = 0
		r.nextIndex[i] = len(r.log) + 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me

	// Your initialization code here (2A, 2B, 2C).
	// init
	r.votedFor = -1;
	r.currentTerm = 0
	r.coreEventChan = make(chan *event, 256)
	r.networkEventChan = make(chan *event, 256)
	r.applyChan = applyCh
	r.state = Follower
	r.stopped = make(chan bool)
	r.log = make([]LogEntry, 0)
	r.nextIndex = make([]int, len(r.peers))
	r.matchIndex = make([]int, len(r.peers))
	r.commitIndex = 0
	r.lastApplied = 0
	DPrintf(r.me, r.currentTerm, r.state,"start")

	r.routineGroup.Add(1)
	go func() {
		defer r.routineGroup.Done()
		r.networkWorkerLoop()
	}()

	r.routineGroup.Add(1)
	go func() {
		defer r.routineGroup.Done()
		r.eventLoop()
	}()

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	return r
}