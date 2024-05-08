package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	data   map[string]string
	record map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.record[args.ReqId]; ok {
		reply.Value = value
		return
	}
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		kv.record[args.ReqId] = reply.Value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.record[args.ReqId]; ok {
		reply.Value = value
		return
	}
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		kv.record[args.ReqId] = reply.Value
	}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.record[args.ReqId]; ok {
		reply.Value = value
		return
	}
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		kv.record[args.ReqId] = reply.Value
	}
	kv.data[args.Key] = kv.data[args.Key] + args.Value
}

func (kv *KVServer) DoneGet(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.record, args.ReqId)
}

func (kv *KVServer) DonePut(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.record, args.ReqId)
}

func (kv *KVServer) DoneAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.record, args.ReqId)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string, 20)
	kv.record = make(map[int64]string)
	return kv
}
