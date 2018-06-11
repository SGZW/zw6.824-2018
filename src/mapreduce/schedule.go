package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	freeWorkerChan := make(chan string, 5)
	go func() {
		for {
			worker := <-registerChan
			freeWorkerChan <- worker
		}
	}()
	var wg sync.WaitGroup
	index := 0
	for {
		select {
		case wk := <-freeWorkerChan:
			if index >= ntasks {
				break
			}
			if phase == mapPhase {
				wg.Add(1)
				go func(i int, free chan string, wkAddr string) {
					call(wkAddr, "Worker.DoTask", DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}, nil)
					wg.Done()
					free <- wkAddr
				}(index, freeWorkerChan, wk)
			} else {
				wg.Add(1)
				go func(i int, free chan string, wkAddr string) {
					call(wkAddr, "Worker.DoTask", DoTaskArgs{jobName, "", phase, i, n_other}, nil)
					wg.Done()
					free <- wkAddr
				}(index, freeWorkerChan, wk)
			}
			index++
		}
		if index >= ntasks {
			break
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
