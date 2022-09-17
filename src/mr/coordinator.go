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
	mm    sync.Mutex
	rm    sync.Mutex
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
	FileName string
	FileId   int
	// startTime int64

	//1.未开始  2.正在执行  3.map phase 阶段已完成  4.reduce phase阶段在执行， 5.reduce phase阶段完成
	Status int
}

// Your code here -- RPC handlers for the worker to call.

	// worker请求任务：
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, reply *HeartbeatReply) error {
	msg := HeartbeatMsg{reply, make(chan struct{})}
	// log.Printf("Heartbeat received worker...\n")
	c.HeartbeatCh <- msg
	<-msg.ok
	log.Printf("Heartbeat complete...reply:%v\n",reply)
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, reply *ReportReply) error {
	msg := reportMsg{request, make(chan struct{})}
	log.Printf("Worker report message...request:%v\n",request)
	c.reportCh <- msg
	<-msg.ok
	// log.Printf("Worker report message complete...")
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
				h := ihash(task.FileName) % (c.nMap - 1)
				if h + 1  == c.X && task.Status == 1{
					c.mapTasks[i].Status = 2
					replyTask = append(replyTask,task)
				}
			}
			// log.Printf("replyTask: %v\n",replyTask)
			c.hearbeatReply(reply,"MapJob",replyTask)
			// log.Printf("map reply:%v\n",reply)
			return
		}

		//2.如果所有任务已经开始且没有超时，worker等待
		isWait := true
		for _, task := range c.mapTasks {
			// task.Status != 2 &&
			if  task.Status == 3 {
				isWait = false
			}
		}
		if isWait {
			log.Printf("等待map阶段...%v\n",c)
			c.hearbeatReply(reply,"WaitJob",[]Task{})
			return 
		}

		//3. map阶段完成，进入reduce phase阶段
		c.phase = "reduce"
		log.Printf("进入reduce阶段...")
	case "reduce":
		if c.Y <= c.nReduce {
			c.Y++
			//把取走的任务标志为正在执行的状态
			var replyTask []Task
			for i, task := range c.reduceTasks {
				ss := strings.Split(task.FileName, "-")
				s, err := strconv.Atoi(ss[2])
				if err != nil {
					log.Fatalf("conv to int error:%s", err.Error())
				}
				if s == c.Y {
					c.reduceTasks[i].Status = 2
					replyTask = append(replyTask,task)
				}
			}
			//响应给worker
			c.hearbeatReply(reply,"ReduceTask",replyTask)
			return 
		}

		isWait := true
		for _, task := range c.reduceTasks {
			// task.Status != 2 && 
			if task.Status == 3 {
				isWait = false
			}
		}
		if isWait {
			log.Printf("等待reduce阶段...%v\n\n",c)
			c.hearbeatReply(reply,"WaitJob",[]Task{})
			return 
		}
		log.Printf("reduce阶段完成....")
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
		log.Printf("received map report... %v",request)
		for fileId := range request.FileIds {
			c.mapTasks[fileId].Status = 3
		}
		//待完成的任务name
		for _, filename := range request.FileNames {
			t := Task{FileId:len(c.reduceTasks) + 1,FileName: filename, Status: 1}
			c.reduceTasks = append(c.reduceTasks,t)
		}
		return

	case "reduce":
		log.Printf("received reduce report...%v",request)
		for fileId := range request.FileIds {
			c.reduceTasks[fileId].Status = 3
		}
		return
	}
}

func (c *Coordinator) hearbeatReply(reply *HeartbeatReply,jobType string,tasks []Task){
	reply.JobType = jobType
	reply.Tasks = tasks
	reply.X = c.X
	reply.Y = c.Y
	reply.NReduce = c.nReduce
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
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
		if task.Status != 3 {
			isComplete = false
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != 3 {
			isComplete = false
		}
	}
	if isComplete{
		log.Printf("coordinartor is complete:%v!\n",c)
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
		c.mapTasks[i].FileName = v
		c.mapTasks[i].FileId = i
		c.mapTasks[i].Status = 1
	}
	go c.schedule()
	c.server()

	return &c
}
