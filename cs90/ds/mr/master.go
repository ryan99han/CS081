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
	mutex           		sync.Mutex
    numWorkers      		int
    files           		[]string
    isDone 					bool

    mapIndex				int
    reduceIndex				int
    mapTasksFinished 		int
    reduceTasksFinished 	int
    nMap					int
    nReduce					int
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

    if m.mapIndex < m.nMap {
    	reply.IsMapTask = true
    	reply.MapIndex = m.mapIndex
    	reply.Filename = m.files[m.mapIndex]
    	m.mapIndex += 1
    } else if m.mapTasksFinished < m.nMap {
    	reply.IsWaiting = true
    } else if m.reduceIndex < m.nReduce {
    	reply.IsMapTask = false
    	reply.ReduceIndex = m.reduceIndex
    	m.reduceIndex += 1
    } else if m.reduceTasksFinished < m.nReduce {
    	reply.IsWaiting = true
    } else {
    	reply.IsDone = true
    	m.isDone = true
    }

    reply.NReduce = m.nReduce
    reply.NMap = m.nMap
    return nil
}

func (m *Master) FinishTaskCall(args *FinishTaskArgs, reply *FinishTaskReply) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    if args.IsMapTask {
    	m.mapTasksFinished += 1
    } else {
    	m.reduceTasksFinished += 1
    }
    return nil
}

//
// Start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	fmt.Println("Starting RPC Server!")
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", Port)
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	return m.isDone
}

//
// Create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
    m.mutex = sync.Mutex{}
    m.files = files
    m.numWorkers = 0 
    m.isDone = false

    m.mapIndex = 0
    m.reduceIndex = 0
    m.mapTasksFinished = 0
    m.reduceTasksFinished = 0
    m.nReduce = nReduce
    m.nMap = len(files)

	m.server()
	return &m
}
