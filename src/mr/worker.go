package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// pid 作为 workerId
	id := os.Getpid()
	lastTaskId := -1
	lastTaskType := ""
	for {
		req := TaskRequest{
			WorkerId:     id,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		rsp := TaskResponse{}
		call("Coordinator.GetTask", &req, &rsp)
		fmt.Println("[worker] wid = ", id, ", rsp = ", rsp)
		switch rsp.TaskType {
		case "MAP":
			fmt.Println("[worker][map] pid = ", id)
			_map(id, rsp.TaskId, rsp.NReduce, rsp.Filepath, mapf)
		case "REDUCE":
			fmt.Println("[worker][reduce] pid = ", id, ", tid = ", rsp.TaskId)
			_reduce(id, rsp.TaskId, rsp.NMap, reducef)
			fmt.Println("[worker][reduce][done] pid = ", id, ", tid = ", rsp.TaskId)
		default:
			fmt.Println("[worker][done] pid = ", id)
			return
		}
		lastTaskId = rsp.TaskId
		lastTaskType = rsp.TaskType
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func _map(workerId int, mapId int, nReduce int, filepath string, mapf func(string, string) []KeyValue) {
	file, _ := os.Open(filepath)
	content, _ := io.ReadAll(file)
	file.Close()

	kva := mapf(filepath, string(content))
	result := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		result[reduceId] = append(result[reduceId], kv)
	}
	for reduceId, kvs := range result {
		outFile, _ := os.Create(mapTempFilename(mapId, reduceId, workerId))
		for _, kv := range kvs {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
}

func _reduce(workerId int, reduceId int, nMap int, reducef func(string, []string) string) {
	var lines []string
	for mapId := 0; mapId < nMap; mapId++ {
		file, err := os.Open(mapOutputFilename(mapId, reduceId))
		if err != nil {
			continue
		}
		fmt.Printf("[worker][reduce] mid = %d, rid = %d\n", mapId, reduceId)
		content, err := io.ReadAll(file)
		if err != nil {
			continue
		}
		file.Close()
		lines = append(lines, strings.Split(string(content), "\n")...)
	}

	result := make(map[string][]string)
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		key := split[0]
		value := split[1]
		result[key] = append(result[key], value)
	}

	keys := make([]string, 0)
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	outFile, _ := os.Create(reduceTempFilename(reduceId, workerId))
	for _, key := range keys {
		if reduceId == 2 && result[key][0] == "x" {
			for i := 0; i < len(result[key]); i++ {
				pop_value := result[key][:len(result[key])-i]
				fmt.Printf("[worker][reduce] wid = %d, rid = %d, key = %v, value = %v, pop_result = %v\n", workerId, reduceId, key, pop_value, reducef(key, pop_value))
			}
		}
		fmt.Fprintf(outFile, "%v %v\n", key, reducef(key, result[key]))
	}
	outFile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
