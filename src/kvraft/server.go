package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opname string
	Key string
	Value string

	ClientId int64
	Seq int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdatabase map[string]string	// kv database
	detectDup map[int64]int		// detect duplicate
	chanresult map[int]chan Op	// to receive op data
}

// wait raft syschronized result
// if success, return OK
// else return ErrWrongLeader or ErrTimeout
func (kv *KVServer) StartCommand(oop Op) Err {
	kv.mu.Lock()
    kv.mu.Unlock()
	// command has already  been executed
	if seq, ok := kv.detectDup[oop.ClientId]; ok && seq >= oop.Seq {
		return OK
	}

	// apppend client request to raft log and replicate leader's log entries
	index, _, isLeader := kv.rf.Start(oop)
	if !isLeader {
		return ErrWrongLeader
	}
	// use chanresult and wait raft syschronized result
	ch := make(chan Op)
	chanresult[index] = ch
	defer func() {
		delete(kv.chanresult, index)
	}

	// waiting for operation result
	// success or wrongleader or timeout
	select {
	case res := ch:
		if res == oop {
			fmt.Println("Index: ", index, " operate successfully. Reply to client.")
			return OK
		} else {
			return ErrWrongLeader
		}
	case <- time.After(time.Second * 1):
		fmt.Println("Timeout, index is: ", index)
		return ErrTimeout
	}

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{"Get", args.Key, "", args.CliendId, args.Seq}
	reply.Err = kv.StartCommand(op)
	if reply.Err != OK {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value, ok := kv.kvdatabase[args.Key]
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{args.Op, args,Key, args.Value, args.ClientId, args.Seq}
	reply.Err = StartCommand(op)
	if reply.Err != OK {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) CheckDup(ClientId int64, Seq int) bool {
	seq, ok := kv.detectDup[ClientId]
	if ok {
		return Seq > seq
	}
	return true
}

func (kv *KVServer) Apply(oop Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// apply operation
	// only "Put" and "Append" can be applied
	if CheckDup(oop.ClientId, oop.Seq) {
		switch oop.Opname {
		case "Put":
			kv.kvdatabase[oop.Key] = oop.Value
		case "Append":
			if _, ok := kv.kvdatabase[oop.Key]; ok {
				kv.kvdatabase[oop.Key] += oop.Value
			} else {
				kv.kvdatabase[oop.Key] = oop.Value
			}
		}
		kv.detectDup[oop.CliendId] = oop.Seq
	}
}

func (kv *KVServer) Reply(oop Op, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chanresult[index]
	if ok {
		select {
		case <- ch:
		default:
		}
		ch <- oop
	}

}

func (kv *KVServer) doApplyOp() {
	// dead loop
	// ready for new applied entries
	for {
		appliedMsg := <- kv.applyCh
		if appliedMsg.CommandValid {
			index := appliedMsg.CommandIndex
			if oop, ok := appliedMsg.Command.(Op); ok {
				kv.Apply(oop)
				kv.Reply(oop)
			}
		}

	}
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	rf.kvdatabase = make(map[string]string)
	rf.detectDup = make(map[int64]int)
	rf.chanresult = make(map[int]chan Op)

	go kv.doApplyOp()

	return kv
}
