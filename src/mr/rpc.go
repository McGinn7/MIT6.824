package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	Id       int
	Type     string
	Filepath string
	TTL      time.Time // 超时时间点
}

type TaskRequest struct {
	WorkerId     int
	LastTaskId   int
	LastTaskType string
}

type TaskResponse struct {
	TaskId   int
	TaskType string
	Filepath string
	NMap     int
	NReduce  int
}

func mapTempFilename(mapId int, reduceId int, workerId int) string {
	return fmt.Sprintf("mr-map-%d-%d-%d", mapId, reduceId, workerId)
}

func mapOutputFilename(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-map-%d-%d", mapId, reduceId)
}

func finishMapTask(workerId int, mapId int, nReduce int) {
	for reduceId := 0; reduceId < nReduce; reduceId++ {
		os.Rename(
			mapTempFilename(mapId, reduceId, workerId),
			mapOutputFilename(mapId, reduceId))
	}
}

func reduceTempFilename(reduceId int, workerId int) string {
	return fmt.Sprintf("mr-reduce-%d-%d", reduceId, workerId)
}

func reduceOutputFilename(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

func finishReduceTask(workerId int, reduceId int) {
	os.Rename(
		reduceTempFilename(reduceId, workerId),
		reduceOutputFilename(reduceId))
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
