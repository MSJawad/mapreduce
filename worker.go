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

//
// Map functions return a slice of KeyValue.
//
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

	mapCall(mapf)
}

func mapCall(mapf func(string, string) []KeyValue) {

	var sleeper int
	_, err := fmt.Scanf("%d", &sleeper)
	if err != nil {
		log.Fatalf("error reading")
	}

	reply := caller(requestJob)
	fmt.Println(reply.Filename)
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", err)
	}

	kva := mapf(reply.Filename, string(content))
	fmt.Println(kva)

	reply = caller(finishMapJob)
	time.Sleep(time.Second * time.Duration(sleeper))

	fmt.Println("tasks done:", reply.Filename)

}

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
