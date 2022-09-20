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
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
)


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

	status	int //1:follower  2:candidate  3:leader

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent stae on all servers
	currentTerm int
	votedFor    int
	// log       []logger

	//volatile state on all servers
	// commitIndex int
	lastApplied int

	//volatile state on leaders
	// nextIndex []int
	// matchIndex []int
}

type logger struct{
	Index int
	Term  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool	//default is false
	// Your code here (2A).
	term = rf.currentTerm

	if rf.status == 3{
		isleader = true
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
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int		   //currentTerm,for candidate to update itself
	VoteGranted	bool      //true  means candidate recevied vote
}

type AppendEntriesArgs struct{
	Term   int
	LeaderId  int
	PrevLogIndex   int
	PreLogTerm    int
	Entries		[]logger  //log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int	  //leader's commitIndex
}

type AppendEntriesReply struct{
	Term 	int 	//currentTerm, for leader to update itself
	Success bool 	//true  if follower contained entry matching prevLogIndex and prevLogTerm
}




//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	
	if rf.currentTerm == args.Term && rf.votedFor == -1{
		//todo:判断args的log是否满足选举条件，再投票
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return 
	}else if rf.currentTerm < args.Term{
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false 
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lastApplied++
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
	if rf.status != 3{
		isLeader = false
	}

	term = rf.currentTerm


	return index, term, isLeader
}

func (rf *Raft) doHeartbeat() {
	for{
		args := AppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId : rf.me,
			Entries: []logger{},
		}
		reply := AppendEntriesReply{}
		for i := range rf.peers{
			if i == rf.me{
				continue
			}
			rf.sendAppendEntries(i,&args,&reply)
		}
		time.Sleep(time.Duration(70)*time.Millisecond)
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed()  && rf.status == 1{

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		before := rf.lastApplied
		//1.使用rand函数让该结点睡眠一定的时间
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(150)+150
		time.Sleep(time.Duration(n)*time.Millisecond)

		//2.睡眠时间完成后检查期间是否接收到心跳
		if  rf.lastApplied != before{
			continue
		}

		//3.如果没有接收到心跳,成为候选人，向其他结点发起投票
		//• Increment currentTerm
		//• Vote for self
		//• Reset election timer
		//• Send RequestVote RPCs to all other servers
		rf.mu.Lock()
		rf.status = 2
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.mu.Unlock()
		voteRequest := RequestVoteArgs{
			Term:rf.currentTerm,
			CandidateId: rf.me,
			//数组溢出了
			// LastLogIndex:rf.log[len(rf.log)-1].index,
			// LastLogTerm :rf.log[len(rf.log)-1].term,
		}
		voteReply := RequestVoteReply{Term:-1,VoteGranted:false}

		vote := 1
		flag := false	//判断其他结点的任期是否比当前结点大，是则跳出候选

		for i := range rf.peers{
			if i == rf.me{
				continue
			}
			rf.sendRequestVote(i,&voteRequest,&voteReply)

			
			if voteReply.Term > rf.currentTerm{
				flag = true
				rf.status = 1
				break
			}

			if voteReply.VoteGranted{
				vote++
			}
		}

		if flag {
			continue
		}
		
		//选举的三种结果：
		//If votes received from majority of servers: become leader
		//If AppendEntries RPC received from new leader: convert to follower
		//If election timeout elapses: start new election
		startTime := time.Now().UnixNano()
		for {
			if vote > len(rf.peers)/2{
				rf.status = 3
				go rf.doHeartbeat()
			}else if rf.lastApplied != before{
				rf.status = 1
				break
			}else if  time.Now().UnixNano() - startTime / 1e6 > 100{//100毫秒
				rf.status = 1
				break
			}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	
	// Your initialization code here (2A, 2B, 2C).
	rf.status = 1
	rf.currentTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
