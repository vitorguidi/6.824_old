package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//to try=> timestamp task attempt and only accept complete/rename when it is done

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		progressArgs := ReportProgressArgs{}
		progressReply := ReportProgressReply{}

		if !ok || getTaskReply.AllTasksDone {
			continue
		} else if getTaskReply.TaskType == "map" {
			tempFiles, _ := runMap(mapf, &getTaskReply)
			progressArgs.TaskId = getTaskReply.TaskId
			progressArgs.Status = 2
			progressArgs.TempMapFiles = tempFiles
			call("Coordinator.ReportOnMapProgress", &progressArgs, &progressReply)
		} else if getTaskReply.TaskType == "reduce" {
			runReduce(reducef, &getTaskReply)
			progressArgs.TaskId = getTaskReply.TaskId
			progressArgs.Status = 2
			call("Coordinator.ReportOnReduceProgress", &progressArgs, &progressReply)
		} else {
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func runMap(mapf func(string, string) []KeyValue, reply *GetTaskReply) ([]string, error) {
	fileName := reply.File
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	kva := mapf(fileName, string(content))
	data := make([][]KeyValue, reply.NumberOfReducers)
	for _, kv := range kva {
		ind := ihash(kv.Key) % reply.NumberOfReducers
		data[ind] = append(data[ind], kv)
	}
	tempFiles := make([]string, 0)
	for ind, arr := range data {
		tempFileName := fmt.Sprintf("mr-tmp-task:%d-ind:%d", reply.TaskId, ind)
		tempFile, _ := ioutil.TempFile(".", "")
		enc := json.NewEncoder(tempFile)
		for _, keyValuePair := range arr {
			err := enc.Encode(&keyValuePair)
			if err != nil {
				log.Fatalf("cannot encode json %v", keyValuePair.Key)
			}

		}
		os.Rename(tempFile.Name(), tempFileName)
		tempFiles = append(tempFiles, tempFileName)
		tempFile.Close()
	}

	return tempFiles, nil
}

func runReduce(reducef func(string, []string) string, reply *GetTaskReply) {

	tempFiles := reply.TempMapFiles
	outFileName := fmt.Sprintf("mr-out-%d", reply.TaskId)
	outFile, _ := ioutil.TempFile(".", "")

	tmp := make([]KeyValue, 0)

	for _, tempFile := range tempFiles {
		file, err := os.Open(tempFile)
		if err != nil {
			log.Fatalf("cannot open %v", tempFile)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			tmp = append(tmp, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(tmp))
	freq := make(map[string][]string)

	for _, kv := range tmp {
		_, isThere := freq[kv.Key]
		if isThere {
			freq[kv.Key] = append(freq[kv.Key], kv.Value)
		} else {
			val := make([]string, 0)
			val = append(val, kv.Value)
			freq[kv.Key] = val
		}
	}

	for key, val := range freq {
		result := reducef(key, val)
		fmt.Fprintf(outFile, "%v %v\n", key, result)
	}

	os.Rename(outFile.Name(), outFileName)
	outFile.Close()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
