package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		reply := caller(requestJob)
		jobType := reply.JobType
		switch jobType {
		case (mapJob):
			mapCall(&reply, mapf)
		case (noJob):
			fmt.Println("No task recieved")
			time.Sleep(time.Millisecond * 50)
		case (finishAllJobs):
			return
		}
	}
}

//
// handles a map task
//
func mapCall(reply *MyReply, mapf func(string, string) []KeyValue) {

	fmt.Println(reply.Content)
	file, err := os.Open(reply.Content)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", err)
	}

	kva := mapf(reply.Content, string(content))
	//fmt.Println(kva)
	_ = kva

	finishreply := caller(finishedMapJob)

	fmt.Println("task number %v done:", finishreply.Content)

}

//
// requests a msgType job
//
func caller(msgType int) MyReply {
	args := MyArgs{}
	args.MessageType = msgType
	reply := MyReply{}
	call("Master.Handler", &args, &reply)

	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
