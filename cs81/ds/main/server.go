package main

import (
    "fmt"
    "net"
    "net/http"
    "net/rpc"
    "log"
    "cs81/ds/mr"
)

func main() {
    fmt.Println("Starting RPC server!")

    arith := new(mr.Arith)
    rpc.Register(arith)
    rpc.HandleHTTP()
    l, e := net.Listen("tcp", ":1234")
    if e != nil {
        log.Fatal("listen error:", e)
    }
    http.Serve(l, nil)
}
