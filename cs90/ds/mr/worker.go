package mr

import (
    "os"
    "fmt"
    "log"
    "net/rpc"
    "hash/fnv"
    "io/ioutil"
    "encoding/json"
    "strings"
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


func Worker(
    mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	w := worker{}
	w.createWorker(mapf, reducef)
    w.runWorker()	
}

func (w *worker) createWorker(
    mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	w.mapf    = mapf
	w.reducef = reducef

	args := RegisterArgs{}
    reply := RegisterReply{}

    success := call("Master.RegisterCall", &args, &reply)
    if !success {
        log.Fatal("Failed to create worker")
    }

    w.id = reply.Id
    // fmt.Printf("Worker %d created\n", w.id)
}

func (w *worker) runWorker() {
    for {
        args := GetTaskArgs{}
        reply := GetTaskReply{}

        success := call("Master.GetTaskCall", &args, &reply)
        if !success {
            log.Fatal("Failed to get task")
        }

        if reply.IsWaiting {
            continue
        } else if reply.IsDone {
            break
        } else if reply.IsMapTask {
            // fmt.Println("Starting Map Task")
            w.mapTask(reply)
        } else {
            // fmt.Println("Starting Reduce Task")
            w.reduceTask(reply)
        }
    }  
}

func (w *worker) mapTask(reply GetTaskReply) {
    filename := reply.Filename
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("Cannot open %v", filename)
    }

    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
    }

    file.Close()
    kva := w.mapf(filename, string(content))

    // for _, s := range kva {
    //     fmt.Println(s.Key)
    //     fmt.Println(s.Value)
    // }
    // fmt.Println(len(intermediate))

    arr := make([][]KeyValue, reply.NReduce)
    for _, kv := range kva {
        i := ihash(kv.Key) % reply.NReduce 
        arr[i] = append(arr[i], kv)
    }

    for i, row := range arr {
        fileName := fmt.Sprintf("mr-%d-%d", reply.MapIndex, i)

        f, err := os.Create(fileName)
        if err != nil {
            // print msg
            return
        }

        enc := json.NewEncoder(f)
        for _, kv := range row {
            err := enc.Encode(&kv)
            if err != nil {
                // print msg
            }

        }

        err2 := f.Close()
        if err2 != nil {
            // print msg
        }
    }


    finishArgs := FinishTaskArgs{}
    finishReply := FinishTaskReply{}

    finishArgs.IsMapTask = true
    success := call("Master.FinishTaskCall", &finishArgs, &finishReply)
    if !success {
        log.Fatal("Failed to create worker")
    }
    // fmt.Println("Finished Map Task")
}

func (w *worker) reduceTask(reply GetTaskReply) {
    kvs := make(map[string][]string)
    for i := 0; i < reply.NMap; i++ {
        filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceIndex)
        file, err := os.Open(filename)
        if err != nil {
            log.Fatalf("Cannot open %v", filename)
        }

        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            err := dec.Decode(&kv)
            if err != nil {
                break
            }

            _, ok := kvs[kv.Key]
            if !ok {
                kvs[kv.Key] = make([]string, 0)
            }
            kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
        }

        result := make([]string, 0)
        for k, v := range kvs {
            line := fmt.Sprintf("%v %v\n", k, w.reducef(k, v))
            result = append(result, line)
        }

        newFileName := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
        err = ioutil.WriteFile(newFileName, []byte(strings.Join(result, "")), 1000)
        if err != nil {
            // print msg
        }
    }

    finishArgs := FinishTaskArgs{}
    finishReply := FinishTaskReply{}

    finishArgs.IsMapTask = false
    success := call("Master.FinishTaskCall", &finishArgs, &finishReply)
    if !success {
        log.Fatal("Failed to create worker")
    }
    // fmt.Println("Finished Reduce Task")
}



//
// Send an RPC request to the master, wait for the response.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1" + Port)
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
