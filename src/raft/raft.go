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
	"math/rand"
	"sync"
	"time"
)
import "github.com/yipilanglbc/6.824/src/labrpc"

// import "bytes"
// import "labgob"



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

type LogEntry struct{
	Cmd interface{}
	Term int
	//TODO index int ?
}

type State int

type Notify struct {}

const (
	Leader = iota
	Follower
	Candidate
	voteNull = -1
	conflictNull = -1
	electionTimeoutFloor = 700
	electionTimeoutCeil = 1000
	heartBeatInterval = 150
)


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
	//TODO
	currentTerm int
	votedFor int
	log []LogEntry
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	voteCount int
	applyCh chan ApplyMsg
	state State
	chanLeaderElected chan Notify
	chanGetAERPC chan Notify
	chanGrantVote chan Notify
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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
type RequestVoteArgs struct {				//RPC	- the method's type is exported.
											//		- the method is exported.
											//		- the method has two arguments, both exported (or builtin) types.
											//		- the method's second argument is a pointer.
											//		- the method has return type error.
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
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

type AppendEntriesReply struct{
	Term int
	Success bool
	ConflictFirstIndex int
}

func (rf *Raft) upToDate(lastLogTerm int, lastLogIndex int) bool{
	defer un(trace(rf.me, rf.state, rf.currentTerm,"upToDate()"))

	lastIndex := len(rf.log) - 1
	if lastLogTerm > rf.log[lastIndex].Term{
		return true
	}else if lastLogTerm == rf.log[lastIndex].Term && lastLogIndex > lastIndex{
		return true
	}else{
		return false
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer un(trace(rf.me, rf.state, rf.currentTerm,"RequestVote()"))
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm						//for candidate to update itself
	reply.VoteGranted = false
	if args.Term < rf.currentTerm{					// receiver rules #1
		return
	}
	if args.Term > rf.currentTerm{					//severs' rules #2
		rf.convertToFollower(args.Term)
	}
	//TODO need modify
	if rf.votedFor == voteNull || rf.votedFor == args.CandidateId && rf.upToDate(args.LastLogTerm, args.LastLogIndex){ // receiver rules #2
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		dropAndNotify(rf.me, rf.state, rf.currentTerm, rf.chanGrantVote, "chanGrantVote")				//reset the electionTimeoutTimer
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer un(trace(rf.me, rf.state, rf.currentTerm,"AppendEntries()"))
	reply.ConflictFirstIndex = conflictNull
	reply.Term = rf.currentTerm
	reply.Success = false
	lastLogIndex := len(rf.log)-1
	if args.Term < rf.currentTerm{			// Receiver implementation #1
		return
	}
	if args.PrevLogIndex > lastLogIndex{	// treat as if you did have that entry but the term did not match
		reply.ConflictFirstIndex = lastLogIndex + 1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm{						// Receiver implementation #2 && optimized to reduce the number of rejected AppendEntries RPCs
		i, term := args.PrevLogIndex-1, rf.log[args.PrevLogIndex].Term
		for ; rf.log[i].Term == term; i--{}
		reply.ConflictFirstIndex = i + 1
	}
	if args.Term >= rf.currentTerm{										//rule for all severs #2 && candidate receive an AppendRPC
		rf.convertToFollower(args.Term)
	}
	dropAndNotify(rf.me, rf.state, rf.currentTerm, rf.chanGetAERPC, "chanGetAEPRC")										//reset the electionTimeoutTimer
	limit := min(len(args.Entries), lastLogIndex - args.PrevLogIndex)
	for i := 0; i < limit; i++{											// Receiver implementation #3
		if args.Entries[i].Term != rf.log[i+args.PrevLogIndex+1].Term{
			rf.log = rf.log[:i+args.PrevLogIndex+1]
			break
		}
	}
	if args.PrevLogIndex+len(args.Entries) > lastLogIndex{				// Receiver implementation #4
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex{								// Receiver implementation #5
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
		rf.apply()
	}
	reply.Success = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader{
		isLeader = false
	}else{
		term = rf.currentTerm
		index = len(rf.log)
		newLogEntry := LogEntry{command, term}
		rf.log = append(rf.log, newLogEntry)
	}
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
	DPrintf("Make peer: %d", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = voteNull
	rf.log = append(rf.log, LogEntry{Term: -1})
	peerNum := len(peers)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, peerNum)
	rf.matchIndex = make([]int, peerNum)
	rf.applyCh = applyCh
	rf.state = Follower
	rf.chanLeaderElected = make(chan Notify, 1)
	rf.chanGetAERPC = make(chan Notify, 1)
	rf.chanGrantVote = make(chan Notify, 1)
	go rf.stateChange()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) getRandomTimeout() time.Duration{
	defer un(trace(rf.me, rf.state, rf.currentTerm,"getRandomTimeout"))
	timeoutInterval := electionTimeoutCeil - electionTimeoutFloor
	random := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(timeoutInterval) + electionTimeoutFloor
	return time.Duration(random) * time.Millisecond
}

//
func (rf *Raft) stateChange() {
	rf.mu.Lock()
	rf.mu.Unlock()
	for{
		electionTimeout := rf.getRandomTimeout()
		rf.mu.Lock()
		state := rf.state
		DPrintf("peer %v electionTimeout is %v, state is %v", rf.me, electionTimeout, stateName[state])
		rf.mu.Unlock()
		switch state {
		case Leader:
			rf.sendAppendEntries()
			time.Sleep(heartBeatInterval * time.Millisecond)
		case Candidate:
			go rf.election()
			select {
			case <-rf.chanGetAERPC:
			case <-rf.chanGrantVote:
			case <-rf.chanLeaderElected:            //让election()函数在获得majority的投票后尽早退出
			case <-time.After(electionTimeout):
				rf.convertToCandidate()
			}
		case Follower:
			select {
			case <-rf.chanGetAERPC:
			case <-rf.chanGrantVote:
			case <-time.After(electionTimeout):
				rf.convertToCandidate()
			}
		}
	}
}

func (rf *Raft) convertToCandidate()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer un(trace(rf.me, rf.state, rf.currentTerm,"convertToCandidate"))
	if rf.state == Leader{
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
}
func (rf *Raft) convertToLeader(){
	defer un(trace(rf.me, rf.state, rf.currentTerm,"convertToLeader"))
	if rf.state != Candidate{
		return
	}
	rf.state = Leader
	for i := range rf.peers{
		if i != rf.me{
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
	}
}
func (rf *Raft) convertToFollower(term int){
	defer un(trace(rf.me, rf.state, rf.currentTerm,"convertToFollower"))
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = voteNull
}
func (rf *Raft) apply(){
	for ; rf.commitIndex > rf.lastApplied; rf.lastApplied++{
		applyMessage := ApplyMsg{
			true,
			rf.log[rf.lastApplied+1].Cmd,
			rf.lastApplied + 1,
		}
		rf.applyCh <- applyMessage
	}
}
func (rf *Raft) advanceCommitIndex(){
	N := len(rf.log)-1				//rules for Leaders #4
	for ; N > rf.commitIndex; N--{
		num := 0
		for i, n := 0, len(rf.peers); i < n; i++{
			if rf.matchIndex[i] >= N{
				num++
			}
		}
		if num > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm{
			rf.commitIndex = N
			//TODO 通知 applier goroutine, sync.Cond
			rf.apply()
			break
		}
	}
}
func (rf *Raft) sendAppendEntries(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer un(trace(rf.me, rf.state, rf.currentTerm,"heatBeat()"))
	entries := make([]LogEntry, 0)
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId:rf.me,
		Entries:entries,
		LeaderCommit: rf.commitIndex,
	}
	for i, peer := range rf.peers{
		if i != rf.me{
			DPrintf("ask peer %v call AppendEntries without entries out of go func\n", i)
			go func(args AppendEntriesArgs, peer *labrpc.ClientEnd, peerNum int){
				rf.mu.Lock()
				if rf.state != Leader{
					rf.mu.Unlock()
					return
				}
				args.PrevLogIndex = rf.nextIndex[peerNum] - 1				//including rules for Leaders #3 && heartBeat
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerNum]:len(rf.log)]...)
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				//for{
				//	ok := peer.Call("Raft.AppendEntries", &args, &reply)
				//	if ok{
				//		break
				//	}
				//}
				ok := peer.Call("Raft.AppendEntries", &args, &reply)
				if !ok{
					return
				}
				DPrintf("break for, ask peer %v call AppendEntries without entries in go func\n", peerNum)
				rf.mu.Lock()
				if args.Term == rf.currentTerm{					//drop old rpc reply
					if !reply.Success{
						if reply.Term > rf.currentTerm{
							rf.convertToFollower(reply.Term)
						}else{									//rules for Leaders #3.2
							rf.nextIndex[peerNum] = reply.ConflictFirstIndex
						}
					}else{										//rules for Leaders #3.1
						rf.matchIndex[peerNum] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peerNum] = rf.matchIndex[peerNum] + 1
						rf.advanceCommitIndex()
					}
				}
				rf.mu.Unlock()
			}(args, peer, i)
		}
	}
}

func (rf *Raft) election(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer un(trace(rf.me, rf.state, rf.currentTerm,"election()"))
	args := RequestVoteArgs{			//args要先确定，不能在go func中确定，否则参数可能不是此时的状态
		rf.currentTerm,
		rf.me,
		len(rf.log) - 1,
		rf.log[len(rf.log)-1].Term,
	}
	for i, peer := range rf.peers{
		if i != rf.me{
			DPrintf("ask peer %v call requestVote out of go func\n", i)
			go func(args RequestVoteArgs, peer *labrpc.ClientEnd, peerNum int){			//这种匿名函数最好复制来自于外部的变量作为参数，否则可能会引用过时的变量
			rf.mu.Lock()
			if rf.state != Candidate{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			//for{
			//	ok := peer.Call("Raft.RequestVote", &args, &reply)
			//	if ok {
			//		break
			//	}
			//}
			ok := peer.Call("Raft.RequestVote", &args, &reply)
			if !ok {//TODO 另起一个goroutine发送RPC？
				return
			}
			DPrintf("break for, ask peer %v call requestVote in go func\n", peerNum)
			rf.mu.Lock()
			if args.Term == rf.currentTerm{						//drop old rpc reply
				if reply.VoteGranted{
					rf.voteCount++
					if rf.voteCount > len(rf.peers)/2{
						rf.convertToLeader()
						dropAndNotify(i, rf.state, rf.currentTerm, rf.chanLeaderElected, "chanLeaderElected")		//无阻塞,reset the electionTimeoutTimer
					}
					//TODO need modify
				}else if reply.Term > rf.currentTerm{			//rule for all severs #2
					rf.convertToFollower(reply.Term)
				}
			}
			rf.mu.Unlock()
			}(args, peer, i)
		}
	}
}
func dropAndNotify(peerNum int, state State, term int, c chan Notify, name string){
	defer un(trace(peerNum, state, term, "dropAndNotify("+name+")"))
	select {														//保证channel可写入
	case <-c:
	default:
	}
	c<-Notify{}
}
func min(x int, y int) int{
	defer un(trace(-1, 3, -1,"min()"))
	if x > y{
		return y
	}
	return x
}
func trace(number int, state State, term int, s string) (int, State, int, string){
	DPrintf("peer %v, %v, term %v, enter %v\n", number, stateName[state], term, s)
	return number, state, term, s
}
func un(number int, state State, term int, s string){
	DPrintf("peer %v, %v, term %v, leave %v\n", number, stateName[state], term, s)
}
var stateName = []string{"Leader", "Follower", "Candidate", ""}