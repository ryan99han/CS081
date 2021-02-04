package main

//
// Start the master process, which is implemented in ../mr/master.go
// go run mrmaster.go pg*.txt
//

import (
    "cs81/ds/mr"
    "time"
    "os"
    "fmt"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
        fmt.Printf("inside for loop")
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
