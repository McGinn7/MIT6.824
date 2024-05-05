package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock       sync.Mutex
	nMap       int
	nReduce    int
	stage      string
	taskStatus map[int]bool
	taskList   chan Task
	reviewList chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *TaskRequest, rsp *TaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if req.LastTaskType == c.stage {
		switch req.LastTaskType {
		case "MAP":
			finishMapTask(req.WorkerId, req.LastTaskId, c.nReduce)
			c.taskStatus[req.LastTaskId] = true
			if len(c.taskStatus) == c.nMap {
				c.stage = "REDUCE"
				c.taskStatus = make(map[int]bool)
				for i := 0; i < c.nReduce; i++ {
					task := Task{
						Id:       i,
						Type:     "REDUCE",
						Filepath: "",
						TTL:      time.Now().Add(10 * time.Second),
					}
					c.taskList <- task
				}
			}

		case "REDUCE":
			finishReduceTask(req.WorkerId, req.LastTaskId)
			c.taskStatus[req.LastTaskId] = true
			if len(c.taskStatus) == c.nReduce {
				c.stage = "DONE"
				close(c.taskList)
				close(c.reviewList)
			}
		}
	}

	for {
		task, ok := <-c.taskList
		if !ok {
			return nil
		}
		if finished, ok := c.taskStatus[task.Id]; ok && finished {
			continue
		} else {
			rsp.TaskId = task.Id
			rsp.TaskType = task.Type
			rsp.Filepath = task.Filepath
			rsp.NMap = c.nMap
			rsp.NReduce = c.nReduce

			task.TTL = time.Now().Add(10 * time.Second)
			c.reviewList <- task
			return nil
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.stage == "DONE" {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	bufferSize := len(files)
	if bufferSize < nReduce {
		bufferSize = nReduce
	}
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		stage:      "MAP",
		taskStatus: make(map[int]bool),
		taskList:   make(chan Task, bufferSize),
		reviewList: make(chan Task, bufferSize),
	}
	fmt.Printf("[coordinator] nMap = %d, nReduce = %d\n", c.nMap, c.nReduce)
	for i, file := range files {
		task := Task{
			Id:       i,
			Type:     "MAP",
			Filepath: file,
			TTL:      time.Now().Add(10 * time.Second),
		}
		c.taskList <- task
	}

	c.server()

	go func() {
		for task := range c.reviewList {
			time.Sleep(time.Until(task.TTL))
			if finished, ok := c.taskStatus[task.Id]; !ok || !finished {
				task.TTL = time.Now().Add(10 * time.Second)
				c.taskList <- task
			}
		}
	}()
	return &c
}
