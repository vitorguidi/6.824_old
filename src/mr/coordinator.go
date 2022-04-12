package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	reduceTasks       []ReduceTask
	mapTasks          []MapTask
	numberReduceTasks int
	tempFiles         [][]string
}

type ReduceTask struct {
	taskId int
	status int
}

type MapTask struct {
	taskId int
	status int
	file   string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) ReportOnMapProgress(args ReportProgressArgs, reply *ReportProgressReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tempFiles[args.TaskId] = args.TempMapFiles
	c.mapTasks[args.TaskId].status = args.Status

	return nil

}

func (c *Coordinator) ReportOnReduceProgress(args ReportProgressArgs, reply *ReportProgressReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reduceTasks[args.TaskId].status = args.Status
	//fmt.Printf("Finalizando task reduce %d com status %d\n", args.TaskId, args.Status)

	return nil
}

func (c *Coordinator) GetTask(args GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.allMapTasksDone() {
		reply.TaskType = "map"
		reply.AllTasksDone = false
		for i, task := range c.mapTasks {
			if task.status == 0 {
				reply.TaskId = i
				reply.File = task.file
				reply.NumberOfReducers = c.numberReduceTasks

				task.status = 1
				c.mapTasks[i] = task
				//fmt.Printf("Status da task %d eh %d\n", i, c.mapTasks[i].status)

				go checkMapCompletion(i, c)

			}
		}

	} else if !c.allReduceTasksDone() {

		reply.TaskType = "reduce"
		reply.AllTasksDone = false

		for i, task := range c.reduceTasks {
			if task.status == 0 {
				reply.TaskId = i
				reply.NumberOfReducers = c.numberReduceTasks

				tempFiles := make([]string, 0)
				for _, taskTempFiles := range c.tempFiles {
					tempFiles = append(tempFiles, taskTempFiles[i])
				}
				reply.TempMapFiles = tempFiles

				task.status = 1
				c.reduceTasks[i] = task

				go checkReduceCompletion(i, c)

			}
		}
	} else {
		reply.AllTasksDone = true
	}
	return nil
}

func checkMapCompletion(id int, c *Coordinator) {
	time.Sleep(10)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTasks[id].status != 2 {
		//fmt.Printf("Map task %d did not finish in time\n", id)
		c.mapTasks[id].status = 0
	}

	//fmt.Printf("Map task %d complete\n", id)
}

func checkReduceCompletion(id int, c *Coordinator) {
	time.Sleep(10)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reduceTasks[id].status != 2 {
		c.reduceTasks[id].status = 0
	}
	//fmt.Printf("Reduce task %d complete\n", id)

}

func (c *Coordinator) allMapTasksDone() bool {
	for _, task := range c.mapTasks {
		if task.status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksDone() bool {
	for _, task := range c.reduceTasks {
		if task.status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allMapTasksDone() && c.allReduceTasksDone()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.mapTasks = make([]MapTask, len(files))
	for i, f := range files {
		c.mapTasks[i] = MapTask{taskId: i, file: f, status: 0}
	}

	c.numberReduceTasks = nReduce
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = ReduceTask{taskId: i, status: 0}
		fmt.Println("")
	}

	c.tempFiles = make([][]string, len(files))

	c.server()
	return &c
}
