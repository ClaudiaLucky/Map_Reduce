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
	//fmt.Println("Scheduleeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	//fmt.Println("jobName: ", jobName)
	//fmt.Println("Phase: ", phase)
	//fmt.Println("mapFiles: ", mapFiles)
	var wg sync.WaitGroup

	if phase == mapPhase {
		wg.Add(ntasks)
		for i, j := range mapFiles {

			arg := DoTaskArgs{jobName, j, phase, i, n_other}
			var reply ShutdownReply
			var ok bool
			go func() {
				v, _ := <-registerChan
				ok = call(v, "Worker.DoTask", arg, &reply)
				if ok == true {
					go func() { registerChan <- v }()
					wg.Done()
				}
				for ok == false {
					v, _ = <-registerChan

					ok = call(v, "Worker.DoTask", arg, &reply)

					if ok == true {
						go func() { registerChan <- v }()
						wg.Done()
						break
					}
				}
			}()
		}
		wg.Wait()
	} else if phase == reducePhase {
		wg.Add(nReduce)
		for i := 0; i < nReduce; i++ {
			arg := DoTaskArgs{jobName, "", phase, i, n_other}
			var reply ShutdownReply
			var ok bool
			go func() {
				v, _ := <-registerChan
				ok = call(v, "Worker.DoTask", arg, &reply)

				if ok == true {
					go func() {
						registerChan <- v
					}()
					wg.Done()
				}
				for ok == false {
					v, _ = <-registerChan
					//fmt.Println("new worker redoOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO: ", v)

					//fmt.Println("PASSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSsss")

					ok = call(v, "Worker.DoTask", arg, &reply)
					if ok == true {
						go func() { registerChan <- v }()
						wg.Done()
						break
					}
				}
			}()

		}
		wg.Wait()

	}
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	//
	fmt.Printf("Schedule: %v done\n", phase)
	//fmt.Printf("\n\n\n")
}
