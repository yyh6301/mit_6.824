package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

/*
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go wc.so
cat mr-out-* | sort | more
*/
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's Heartbeat %v \n", response)
		switch response.JobType {
		case "MapJob":
			doMapTask(response, mapf)
		case "ReduceJob":
			doReduceTask(response, reducef)
		case "WaitJob":
			time.Sleep(time.Second)
		case "CompleteJob":
			return
		default:
			log.Fatalf("unknow job type: %s", response.JobType)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}
func doHeartbeat() HeartbeatReply {
	request := HeartbeatRequest{}
	reply := HeartbeatReply{}
	ok := call("Coordinator.Heartbeat", &request, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return HeartbeatReply{}
}

func doMapTask(reply HeartbeatReply, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}

	//读取该map所属的文件
	for _, task := range reply.tasks {
		file, err := os.Open(task.fileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.fileName)
		}
		file.Close()
		kva := mapf(task.fileName, string(content))
		intermediate = append(intermediate, kva...)
	}
	var interFiles []string
	//把intermediate存到文件中 mr-X-Y 中间文件中
	for _, kv := range intermediate {
		Y := ihash(kv.Key) % reply.nReduce
		filename := fmt.Sprintf("mr-%d-%d", reply.X, Y)
		interFiles  = append(interFiles,filename)
		//先打开文件，如果不存在，则创建
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("encode file error:%v", err.Error())
		}
	}

	var completeFiles []int
	for _, task := range reply.tasks {
		completeFiles = append(completeFiles, task.fileId)
	}

	//汇报工作
	request := ReportRequest{
		fileIds: completeFiles,
		fileNames: interFiles,
	}
	reportReply := ReportReply{}
	ok := call("Coordinator.Report", &request, &reportReply)
	if ok {
		fmt.Printf("map report ok\n")
	} else {
		fmt.Printf("map report call failed!\n")
	}

}

func doReduceTask(reply HeartbeatReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for i := 1; i <= reply.X; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.Y)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open file error:%v", err.Error())
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d",reply.Y)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()

	var doneIds = make([]int,0)
	for _, t := range reply.tasks{
		doneIds = append(doneIds,t.fileId)
	}
	
	//reduce的汇报工作
	request := ReportRequest{
		fileIds:doneIds,
	}
	reportReply := ReportReply{}
	ok := call("Coordinator.Report", &request, &reportReply)
	if ok {
		fmt.Printf("map report ok\n")
	} else {
		fmt.Printf("map report call failed!\n")
	}


}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
