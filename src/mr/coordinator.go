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
	nMap       int

	X int //map ID
	Y int //reduce ID

	//map phase   reduce phase
	phase string


	HeartbeatCh chan HeartbeatMsg
	reportCh    chan reportMsg
	// doneCh      chan struct{}
}

type HeartbeatMsg struct {
	reply *HeartbeatReply
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
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, reply *HeartbeatReply) error {
	msg := HeartbeatMsg{reply, make(chan struct{})}
	log.Printf("Heartbeat received worker...\n")
	c.HeartbeatCh <- msg
	<-msg.ok
	log.Printf("Heartbeat complete...\n")
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, reply *ReportReply) error {
	msg := reportMsg{request, make(chan struct{})}
	log.Printf("Worker report message...\n")
	c.reportCh <- msg
	<-msg.ok
	log.Printf("Worker report message complete...")
	return nil
}

func (c *Coordinator) schedule() {
	// c.initMapPhase()
	for {
		select {
		case msg := <-c.HeartbeatCh:
                c.doHeartbeat(msg.reply)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
                c.doReport(msg.request)
			msg.ok <- struct{}{}
		}
	}
}



func (c *Coordinator) doHeartbeat(reply *HeartbeatReply){
	switch c.phase {
	case "map":
		// 1.map phase 阶段，把input files 划分成 N个splice，分配给map
		if c.X <= c.nMap{
			c.X++
			var replyTask []Task
			for i,task := range c.mapTasks{
				h := ihash(task.fileName) % c.nMap
				if h == c.X && task.status == 1{
					c.mapTasks[i].status = 2
					replyTask = append(replyTask,task)
				}
			}
			c.hearbeatReply(reply,"MapJob",replyTask)
			return
		}

		//2.如果所有任务已经开始且没有超时，worker等待
		isWait := true
		for _, task := range c.mapTasks {
			// task.status != 2 &&
			if  task.status == 3 {
				isWait = false
			}
		}
		if isWait {
			c.hearbeatReply(reply,"WaitJob",[]Task{})
			return 
		}

		//3. map阶段完成，进入reduce phase阶段
		c.phase = "reduce"
	case "reduce":
		if c.Y <= c.nReduce {
			c.Y++
			//把取走的任务标志为正在执行的状态
			var replyTask []Task
			for i, task := range c.reduceTasks {
				ss := strings.Split(task.fileName, "-")
				s, err := strconv.Atoi(ss[2])
				if err != nil {
					log.Fatalf("conv to int error:%s", err.Error())
				}
				if s == c.Y {
					c.reduceTasks[i].status = 2
					replyTask = append(replyTask,task)
				}
			}
			//响应给worker
			c.hearbeatReply(reply,"ReduceTask",replyTask)
			return 
		}

		isWait := true
		for _, task := range c.reduceTasks {
			// task.status != 2 && 
			if task.status == 3 {
				isWait = false
			}
		}
		if isWait {
			c.hearbeatReply(reply,"WaitJob",[]Task{})
			return 
		}
		c.hearbeatReply(reply,"CompleteJob",[]Task{})
		return 
	}
	 
}

// worker汇报任务：
func (c *Coordinator) doReport(request *ReportRequest){
	switch c.phase {
	case "map":
		//map时期提交的汇报
		//完成任务的id
		for fileId := range request.fileIds {
			c.mapTasks[fileId].status = 3
		}
		//待完成的任务name
		for _, filename := range request.fileNames {
			t := Task{fileId:len(c.reduceTasks) + 1,fileName: filename, status: 1}
			c.reduceTasks = append(c.reduceTasks,t)
		}
		return

	case "reduce":
		for fileId := range request.fileIds {
			c.reduceTasks[fileId].status = 3
		}
		return
	}
}

func (c *Coordinator) hearbeatReply(reply *HeartbeatReply,jobType string,tasks []Task){
	reply.JobType = jobType
	reply.tasks = tasks
	reply.X = c.X
	reply.Y = c.Y
	reply.nReduce = c.nReduce
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
		if task.status != 3 {
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
		nMap :  10,
		X: 0,
		Y: 0,
		mapTasks:  make([]Task,len(files)),
		reduceTasks: make([]Task,0), 
		HeartbeatCh: make(chan HeartbeatMsg),
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
