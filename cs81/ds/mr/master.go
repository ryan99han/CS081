package mr

import (
	"fmt"
	"log"
	"sync"
	"net"
	"net/rpc"
	"net/http"
)


type Master struct {
	mutex           sync.Mutex
    numWorkers      int
    files           []string
    filesProcessed  int
}

func (m *Master) RegisterCall(args *RegisterArgs, reply *RegisterReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    m.numWorkers += 1
    reply.Id = m.numWorkers
    return nil
}

func (m *Master) GetTaskCall(args *GetTaskArgs, reply *GetTaskReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()
    filename := ""
    if m.filesProcessed < len(m.files) {
    	filename = m.files[m.filesProcessed]
    }
    reply.Filename = filename
    m.filesProcessed += 1
    return nil
}

//
// Start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	fmt.Println("Starting RPC Server!")
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
    fmt.Println("Inside done")
	ret := true 
    // ret = true
	// Your code here.


	return ret
}

//
// Create a Master.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
    m.mutex = sync.Mutex{}
    m.numWorkers = 0 
    m.files = files
    m.filesProcessed = 0

	m.server()
	return &m
}
