package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//
// Master class
//
type Master struct {
	myLock      sync.Mutex
	allocated   []string
	unallocated []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// Alternate handler used for now to handle worker connections
//
func (m *Master) Handler(args *MyArgs, reply *MyReply) error {
	msg := args.MessageType
	switch msg {
	case (requestJob):
		if len(m.unallocated) > 0 {
			fmt.Println("Worker Connection recieved")
			m.myLock.Lock()

			newFile := m.unallocated[len(m.unallocated)-1]
			reply.Filename = newFile
			m.allocated = append(m.allocated, newFile)
			m.unallocated = m.unallocated[:len(m.unallocated)-1]

			m.myLock.Unlock()

			return nil
		}
		return errors.New("All Map tasks done")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.unallocated = files

	// Your code here.

	m.server()
	return &m
}
