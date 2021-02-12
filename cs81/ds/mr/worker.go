package mr

import (
    "os"
    "fmt"
    "log"
    "net/rpc"
    "hash/fnv"
    "io/ioutil"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id		int
	mapf	func(string, string) []KeyValue
	reducef func(string, []string) string
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

	// Your worker implementation here.
	w := worker{}
	w.register(mapf, reducef)
    w.getTask()	

}

func (w *worker) register(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w.mapf    = mapf
	w.reducef = reducef

	args := RegisterArgs{}
    reply := RegisterReply{}

    success := call("Master.RegisterCall", &args, &reply)
    if !success {
        log.Fatal("Failed to create worker")
    }
    w.id = reply.Id
    fmt.Printf("Worker %d created\n", w.id)
}

func (w *worker) getTask() {
	args := GetTaskArgs{}
    reply := GetTaskReply{}

    success := call("Master.GetTaskCall", &args, &reply)
    if !success {
        log.Fatal("Failed to create worker")
    }

    filename := reply.Filename
    if filename != "" {
        intermediate := []KeyValue{}
        file, err := os.Open(filename)
        if err != nil {
            log.Fatalf("cannot open %v", filename)
        }

        content, err := ioutil.ReadAll(file)
        if err != nil {
            log.Fatalf("cannot read %v", filename)
        }

        file.Close()
        kva := w.mapf(filename, string(content))
        intermediate = append(intermediate, kva...)

        for _, s := range intermediate {
            fmt.Println(s.Key)
            fmt.Println(s.Value)
        }
        fmt.Println(len(intermediate))
    }

    
}

//
// Send an RPC request to the master, wait for the response.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
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
