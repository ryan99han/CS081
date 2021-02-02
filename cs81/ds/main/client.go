package main

import (
    "fmt"
    "net/rpc"
    "log"
    "cs81/ds/mr"
)

func main() {
    client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
    if err != nil {
        log.Fatal("dialing:", err)
    }

    args := &mr.Args{7,8}
    var reply int
    err = client.Call("Arith.Multiply", args, &reply)
    if err != nil {
        log.Fatal("arith error:", err)
    }
    fmt.Printf("Arith: %d*%d=%d\n", args.A, args.B, reply)
}