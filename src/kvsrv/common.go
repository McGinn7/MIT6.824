package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	ReqId int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key   string
	ReqId int64
}

type GetReply struct {
	Value string
}
