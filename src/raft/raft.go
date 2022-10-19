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
	"crypto/rand"
	"math/big"
	mrand "math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type State string

const (
	Leader    State = "leader"
	Follower        = "follower"
	Candidate       = "candidate"
)

type raftstate struct {
	currentTerm int
	votedFor    int
	log         []string
}

type LogEntry struct {
	Command string
	Termid  int
	Index   int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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

	currentTerm int
	votedFor    int
	logs        []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	state       State
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Reinitialized after election
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int

	electionTimer *time.Timer
	// stateCond sync.Cond
}

func (rf *Raft) changeToFollower() {
	rf.state = Follower
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
			//DPrintf("[%v]timer到期但是没被接收", rf.me)
		default:
			//DPrintf("[%v]timer到期并且被接收", rf.me)
		}
	} else {
		//DPrintf("[%v]timer没到期被重置", rf.me)
	}
	mrand.Seed(time.Now().UnixNano())
	rf.electionTimer.Reset(time.Duration(mrand.Intn(300)+400) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) || args.LastLogIndex < lastLogIndex || args.LastLogTerm < lastLogTerm {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.changeToFollower()
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
		}
	}
	//DPrintf("shoudao reqvote[%v],%v,%v,%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	rf.mu.Unlock()
	// rf.mu.Lock()
	// currentstate := rf.state
	// term := rf.currentTerm
	// votedFor := rf.votedFor
	// rf.mu.Unlock()
	// //DPrintf("%v,%v,%v,%v", currentstate, term, votedFor, reply.VoteGranted)
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeToFollower()
	} else {
		rf.changeToFollower()
		//后续的日志检查
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		defer rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	var replys []*AppendEntriesReply
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(serverid int) {
			defer wg.Done()
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(serverid, args, reply)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				replys = append(replys, reply)
			}
		}(i)
	}
	wg.Wait()
	var maxTerm int = -1
	for _, reply := range replys {
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
	}
	rf.mu.Lock()
	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
		rf.votedFor = -1
		rf.changeToFollower()
	}
	rf.mu.Unlock()
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

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.logs) == 0 {
		lastLogIndex = 0
		lastLogTerm = 0
	} else {
		lastLogIndex = rf.logs[len(rf.logs)-1].Index
		lastLogTerm = rf.logs[len(rf.logs)-1].Termid
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) attempElection() {
	rf.mu.Lock()
	// if rf.state != Follower {
	// 	defer rf.mu.Unlock()
	// 	return
	// }
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	//DPrintf("attempt elect[%v],%v,%v,%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	rf.mu.Unlock()
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var wg sync.WaitGroup
	var voteCount int = 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(serverid int) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(serverid, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.changeToFollower()
				return
			}
			if !reply.VoteGranted {
				return
			}
			voteCount++
			if voteCount >= len(rf.peers)/2 && rf.state == Candidate {
				rf.state = Leader
			}
		}(i)
	}
	wg.Wait()
	/*
		这里该用rf.currentTerm还是currentTerm(这是上面刚进入AttemptElection获取到的term值)？
		问题出现在这里整个的AttemptElection的过程并不是全过程Raft状态一致的
			因为不能全过程rf.mu.Lock(),那样会出现死锁
			->s1 AttemptElection持有锁的同时收到s2的requestVote，requestVote尝试获得锁
			->s2 AttemptElection持有锁的同时收到s1的requestVote，requestVote尝试获得锁
			->出现死锁，所以不能在RPC调用的全过程持有锁
		造成RPC前后两次的Raft cuurentTerm以及state不一定一致
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//重置electiontimeout，比如平票的情况，candidate需要重置为follower等待下一轮超时选举
	if voteCount < len(rf.peers)/2 {
		rf.changeToFollower()
	}
	// rf.stateCond.Broadcast()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) ticker() {
	//Candidate和Follower
	heartBeatTimer := time.NewTimer(time.Duration(200) * time.Millisecond)
	for rf.killed() == false {
		rf.mu.Lock()
		currentState := rf.state
		//DPrintf("[%v],%v,%v,%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		rf.mu.Unlock()
		switch currentState {
		case Candidate, Follower:
			<-rf.electionTimer.C
			//DPrintf("[%v]timeout", rf.me)
			rf.attempElection()
		case Leader:
			<-heartBeatTimer.C
			rf.heartBeats()
			heartBeatTimer.Reset(time.Duration(200) * time.Millisecond)
		}
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

	//如果是leader，没有timeout
	// rf.mu.Lock()
	// currentstate := rf.state
	// rf.mu.Unlock()
	// if currentstate == Leader {
	// 	time.Sleep(time.Second)
	// 	break
	// }

	//一开始只考虑了作为非leader需要ticker，实际上leader也需要定时发送hearbeat,所以这里不需要leader在这里阻塞，直接判断状态进入不同的计时器即可
	// rf.mu.Lock()
	// for rf.state == Leader {
	// 	//如果当前状态为Leader，那就释放锁，然后阻塞在这里等待其他线程唤醒，唤醒条件就是state从Leader转换为Follower
	// 	rf.notLeaderCond.Wait()
	// }
	// rf.mu.Unlock()
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
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// rf.stateCond = sync.Cond{L: &rf.mu}
	random, _ := rand.Int(rand.Reader, big.NewInt(300))
	number := random.String()
	num, _ := strconv.Atoi(number)
	rf.electionTimer = time.NewTimer(time.Duration(num+400) * time.Millisecond)
	go rf.ticker()

	return rf
}
