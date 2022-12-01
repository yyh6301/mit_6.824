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
		done := make(chan struct{}, 1)
		response := doHeartbeat()
		// log.Printf("Worker: receive coordinator's Heartbeat %v \n", response)
		switch response.JobType {
		case "MapJob":
			go doMapTask(response, done,mapf)
			select{
			case <-done:
				break
			case <-time.After(time.Duration(10 * time.Second)):
				fmt.Println("timeout!!!")
				return
			}

		case "ReduceJob":
			go doReduceTask(response, done,reducef)
			select{
			case <-done:
				break
			case <-time.After(time.Duration(10 * time.Second)):
				fmt.Println("timeout!!!")
				return
			}
		case "WaitJob":
			time.Sleep(3*time.Second)
		case "CompleteJob":
			log.Printf("job complete,exit normally\n")
			return
		default:
			log.Printf("unknow job type: %s\n", response.JobType)
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
func doHeartbeat() *HeartbeatReply {
	request := HeartbeatRequest{}
	reply := HeartbeatReply{}
	call("Coordinator.Heartbeat", &request, &reply)
	//if call faild,assume that the coordinator has exited because the job is done, and so the worker can terminate too.	
	return &reply
}

func doMapTask(reply *HeartbeatReply,done chan struct{}, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}

	//读取该map所属的文件
	for _, task := range reply.Tasks {
		file, err := os.Open(task.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.FileName)
		}
		file.Close()
		kva := mapf(task.FileName, string(content))
		intermediate = append(intermediate, kva...)
	}

	interfileMap := make(map[string][]KeyValue)
	//把intermediate存到文件中 mr-X-Y 中间文件中
	for _, kv := range intermediate {
		Y := ihash(kv.Key) % reply.NReduce
		filename := fmt.Sprintf("mr-%d-%d", reply.X, (Y+1))

		interfileMap[filename] = append(interfileMap[filename],kv)
	}

	var interFiles []string
	for filename,kv := range interfileMap{
		//使用tempfile+rename，保证读写文件的原子性
		file,err := ioutil.TempFile("",filename)
		interFiles  = append(interFiles,filename)
		if err != nil {
			log.Fatalf("create temp file err:%v\n",err.Error())
		}
		enc := json.NewEncoder(file)
		for _, v :=  range kv {
		  err := enc.Encode(&v)
		  if err != nil {
			  log.Fatalf("encode v error:%v",err.Error())
		  }
		}
		err = os.Rename(file.Name(),filename)
		if err != nil{
			log.Fatalf("rename file error:%v\n",err.Error())
		}
	}

	var completeFiles []int
	for _, task := range reply.Tasks {
		completeFiles = append(completeFiles, task.FileId)
	}

	//汇报工作
	request := ReportRequest{
		FileIds: completeFiles,
		FileNames: interFiles,
	}
	reportReply := ReportReply{}
	ok := call("Coordinator.Report", &request, &reportReply)
	if ok {
		fmt.Printf("map report ok:%v\n\n",request)
	} else {
		fmt.Printf("map report call failed!\n")
	}
	done <- struct{}{}

}

func doReduceTask(reply *HeartbeatReply,done chan struct{}, reducef func(string, []string) string) {
	var intermediate []KeyValue
	var doneFileNames []string
	for i := 1; i <= reply.X; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.Y)
		file, err := os.Open(filename)
		doneFileNames = append(doneFileNames,filename)
		if err != nil {
			log.Printf("open file error:%v", err.Error())
			continue
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

	
	//reduce的汇报工作
	request := ReportRequest{
		FileNames:doneFileNames,
	}
	reportReply := ReportReply{}
	ok := call("Coordinator.Report", &request, &reportReply)
	if ok {
		fmt.Printf("reduce report ok，%v\n\n",request)
	} else {
		log.Fatalf("map report call failed!\n")
	}
	done <- struct{}{}
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
