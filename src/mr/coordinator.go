package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

// import "time"

type Coordinator struct {
	// Your definitions here.
	mapTasks      	 []Task
	reduceTasks 	 []Task
	nReduce    int

	X int //map Number
	Y int //reduce Number

	//map phase   reduce phase
	phase string

	// mmu sync.Mutex
	// rmu sync.Mutex

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	// doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartBeatReply
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

type Task struct {
	fileName string
	fileId   int
	// startTime int64

	//1.未开始  2.正在执行  3.map phase 阶段已完成  4.reduce phase阶段在执行， 5.reduce phase阶段完成
	status int
}

// Your code here -- RPC handlers for the worker to call.

	// worker请求任务：
func (c *Coordinator) Heartbeat(request *HeartBeatRequest, response *HeartBeatReply) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportReply) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	// c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
                    c.doHeartBeat(msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
                    c.doReport(msg.request)
			msg.ok <- struct{}{}
		}
	}
}



func (c *Coordinator) doHeartBeat(reply *HeartBeatReply){
	// c.mmu.Lock()
	// defer c.mmu.Unlock()
	log.Printf("HeartBeat received worker...\n")

	switch c.phase {
	case "map":
		// 1.map phase 阶段，判断是否还有未开始的任务或超时的任务
		// var mapTask []Task
	
		for _, task := range c.mapTasks {

			if task.status == 1 {
				// mapTask = append(mapTask, task)
				// if len(mapTask) >= 5 {

				c.X++
				// t := Task{fileName: task.fileName, status: 2, fileId: task.fileId}
				c.mapTasks[task.fileId].status =  2

				reply.JobType = "MapTask"
				reply.tasks = []Task{task}
				reply.X = c.X
				reply.nReduce = c.nReduce
				return 
				// }
			}
		}


		//2.如果所有任务已经开始且没有超时，worker等待
		isWait := true
		for _, task := range c.reduceTasks {
			if task.status != 4 && task.status != 5 {
				isWait = false
			}
		}
		if isWait {
			reply.JobType = "WaitTask"
			return 
		}

		//3. map阶段完成，进入reduce phase阶段
		c.phase = "reduce"
	case "reduce":
		if c.Y <= c.nReduce {
			c.Y++
			//把取走的任务标志为正在执行的状态
			for _, task := range c.reduceTasks {
				ss := strings.Split(task.fileName, "-")
				s, err := strconv.Atoi(ss[2])
				if err != nil {
					log.Fatalf("conv to int error:%s", err.Error())
				}
				if s == c.Y {

					// t := Task{fileName: task.fileName, status: 4, fileId: task.fileId}
					c.reduceTasks[task.fileId].status = 4
				}
			}
			//响应给worker
			reply.Y = c.Y
			reply.JobType = "ReduceTask"
			return 
		}

		isWait := true
		for _, task := range c.reduceTasks {
			if task.status != 4 && task.status != 5 {
				isWait = false
			}
		}

		if isWait {
			reply.JobType = "WaitTask"
			return 
		}

		reply.JobType = "CompleteTask"
		return 
	}
	 
}

// worker汇报任务：
func (c *Coordinator) doReport(request *ReportRequest){


	switch c.phase {
	case "map":
		//map时期提交的汇报
		for fileId := range request.fileIds {

			// task := Task{status: 3}
			c.mapTasks[fileId].status = 3

		}

		for _, filename := range request.fileNames {

			t := Task{fileId:len(c.reduceTasks) + 1,fileName: filename, status: 4}
			c.reduceTasks = append(c.reduceTasks,t)

		}

	case "reduce":
		//TODO 这里代码可以优化成worker端直接把完成的FileID传过来，这边直接改fileid就行，不用再计算
		for _, task := range c.reduceTasks {
			ss := strings.Split(task.fileName, "-")
			s, err := strconv.Atoi(ss[2])
			if err != nil {
				log.Fatalf("conv to int error:%s", err.Error())
			}
			if s == request.Y {

				// t := Task{fileName: task.fileName, status: 5, fileId: task.fileId}
				c.reduceTasks[task.fileId].status = 5 

			}
		}

	}
	
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	log.Printf("coordinator start listen...\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	isComplete := true
	for _, task := range c.mapTasks {
		if task.status != 3 {
			isComplete = false
		}
	}

	for _, task := range c.reduceTasks {
		if task.status != 5 {
			isComplete = false
		}
	}
	if isComplete{
		log.Printf("coordinartor is complete!\n")
	}

	return isComplete
	// return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   "map",
		nReduce: nReduce,
		mapTasks:  make([]Task,len(files)),
		reduceTasks: make([]Task,0), 
		heartbeatCh: make(chan heartbeatMsg),
		reportCh :   make(chan reportMsg),
	}

	// Your code here.

	for i, v := range files {
		c.mapTasks[i].fileName = v
		c.mapTasks[i].fileId = i
		c.mapTasks[i].status = 1
	}
	go c.schedule()
	c.server()

	return &c
}
