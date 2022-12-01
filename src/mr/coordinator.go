package mr

import (
	"sync"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
	"fmt"
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

	mu		sync.Mutex
	// rmu		sync.Mutex
	// HeartbeatCh chan struct{}
	// reportCh    chan struct{}
	doneCh      chan struct{}
}

// type HeartbeatMsg struct {
// 	reply *HeartbeatReply
// 	ok       chan struct{}
// }

// type reportMsg struct {
// 	request *ReportRequest
// 	ok      chan struct{}
// }

type Task struct {
	FileName string
	FileId   int
	StartTime int64

	//1.未开始  2.正在执行  3.map phase 阶段已完成  4.reduce phase阶段在执行， 5.reduce phase阶段完成
	Status int
}

// Your code here -- RPC handlers for the worker to call.

	// worker请求任务：
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, reply *HeartbeatReply) error {
	// msg := HeartbeatMsg{reply, make(chan struct{})}
	// log.Printf("Heartbeat received worker...\n")
	c.mu.Lock()
	c.doHeartbeat(reply)
	c.mu.Unlock()
	// <-c.HeartbeatCh
	// log.Printf("Heartbeat reply to worker:%v\n",reply)
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, reply *ReportReply) error {
	// msg := reportMsg{request, make(chan struct{})}
	// // log.Printf("recevied worker report:%v\n",request)
	// c.reportCh <- msg
	// <-msg.ok
	c.mu.Lock()
	c.doReport(request)
	c.mu.Unlock()
	return nil
}

// func (c *Coordinator) schedule() {
// 	// c.initMapPhase()
// 	for {
// 		select {
// 		case msg := <-c.HeartbeatCh:
// 				c.doHeartbeat(msg.reply)
// 			msg.ok <- struct{}{}
// 		case msg := <-c.reportCh:
// 				c.doReport(msg.request)
// 			msg.ok <- struct{}{}
// 		// case <-c.doneCh:
// 		// 	log.Printf("coordinator done,schedulef exit\n")
// 		// 	return
// 		}
// 	}
// }



func (c *Coordinator) doHeartbeat(reply *HeartbeatReply){
	switch c.phase {
	case "map":
		// 1.map phase 阶段，把input files 划分成 N个splice，分配给map
		if c.X <= c.nMap{
			c.X++
			var replyTask []Task
			for i,task := range c.mapTasks{
				if  task.Status == 1{
					c.mapTasks[i].Status = 2
					c.mapTasks[i].StartTime = time.Now().Unix()
					replyTask = append(replyTask,task)
					break
				}
			}
			c.hearbeatReply(reply,"MapJob",replyTask,c.X,c.Y)
			return
		}

		//2.如果所有任务已经开始且没有超时，worker等待

		isWait := false
		for i, task := range c.mapTasks {
			// task.Status != 2 &&
			lap := time.Now().Unix() - task.StartTime
			//超时,删除
			if lap >= 10 && task.Status == 2{
				for j :=1 ;j <=c.nReduce;j++{
					filename := fmt.Sprintf("mr-%d-%d",i+1,j)
					os.Remove(filename)
					log.Printf("remove file %s\n",filename)
				}
				c.mapTasks[i].StartTime = time.Now().Unix()
				c.hearbeatReply(reply,"MapJob",[]Task{task},i+1,c.Y)
				log.Printf("map timeout reply:%v\n",reply)
				return
			}
			if  task.Status != 3 {
				isWait = true
			}
		}
		if isWait {
			log.Printf("等待map阶段...\n",)
			c.hearbeatReply(reply,"WaitJob",[]Task{},c.X,c.Y)
			return 
		}else{
				//3. map阶段完成，进入reduce phase阶段
			c.phase = "reduce"
			log.Printf("进入reduce阶段...\n当前coordinator状态：%v\n",c)
			c.hearbeatReply(reply,"WaitJob",[]Task{},c.X,c.Y)
		}

		
	case "reduce":
		if c.Y < c.nReduce {
			c.Y++
			//把取走的任务标志为正在执行的状态
			var replyTask []Task
			for i, task := range c.reduceTasks {
				ss := strings.Split(task.FileName, "-")
				s, err := strconv.Atoi(ss[2])
				if err != nil {
					log.Fatalf("conv to int error:%s\n", err.Error())
				}
				if s == c.Y {
					c.reduceTasks[i].Status = 2
					c.reduceTasks[i].StartTime = time.Now().Unix()
					replyTask = append(replyTask,task)
				}
			}
			//响应给worker
			c.hearbeatReply(reply,"ReduceJob",replyTask,c.X,c.Y)
			return 
		}

		isWait := false
		for i, task := range c.reduceTasks {

			lap := time.Now().Unix() - task.StartTime
			//超时,删除
			if lap >= 10  && task.Status == 2{
				r := strings.Split(task.FileName,"-")
				reduceID,_ := strconv.Atoi(r[2])
				filename := fmt.Sprintf("mr-out-%d",reduceID)
				err := os.Remove(filename)
				if err != nil {
					log.Printf("remove file error:%v\n",err.Error())
				}
				
				log.Printf("reduce worker timeout, remove file %s\n",filename)
				c.reduceTasks[i].StartTime = time.Now().Unix()
				c.hearbeatReply(reply,"ReduceJob",[]Task{task},c.X,reduceID)
				log.Printf("reduce timeout reply:%v\n",reply)
				return
			}
			if task.Status != 3 {
				isWait = true
			}
		}
		if isWait {
			log.Printf("等待reduce阶段...\n")
			c.hearbeatReply(reply,"WaitJob",[]Task{},c.X,c.Y)
			return 
		}else{
			log.Printf("reduce阶段完成....")
			c.hearbeatReply(reply,"CompleteJob",[]Task{},c.X,c.Y)
			c.doneCh <- struct{}{}
		}
		
		return 
	}
	 
}

// worker汇报任务：
func (c *Coordinator) doReport(request *ReportRequest){
	switch c.phase {
	case "map":
		//map时期提交的汇报
		//完成任务的id
		// log.Printf("received map report... %v",request)
		for _,fileId := range request.FileIds {
			c.mapTasks[fileId].Status = 3
		}
		//待完成的任务name
		for _, filename := range request.FileNames {
			t := Task{FileId:len(c.reduceTasks) + 1,FileName: filename, Status: 1}
			c.reduceTasks = append(c.reduceTasks,t)
		}
		return

	case "reduce":
		// log.Printf("received reduce report...%v",request)
		for _,filename := range request.FileNames {
			for i := range c.reduceTasks{
				if c.reduceTasks[i].FileName == filename{
					c.reduceTasks[i].Status = 3
				}
			}
		}
		return
	}
}

func (c *Coordinator) hearbeatReply(reply *HeartbeatReply,jobType string,tasks []Task,X,Y int){
	reply.JobType = jobType
	reply.Tasks = tasks
	reply.X = X
	reply.Y = Y
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
	// l, e := net.Listen("tcp", ":1234")
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
	<- c.doneCh 
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
		// log.Printf("coordinartor is complete:%v!\n",c)
		return isComplete
	}

	// log.Printf("coordinartor is working\n mapTasks:%v \n\n reduceTasks:%v \n\n",c.mapTasks,c.reduceTasks)

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
		nMap :  8,
		X: 0,
		Y: 0,
		mapTasks:  make([]Task,len(files)),
		reduceTasks: make([]Task,0), 
		// HeartbeatCh: make(chan HeartbeatMsg),
		// reportCh :   make(chan reportMsg),
		doneCh :  make(chan struct{}),
	}

	// Your code here.

	for i, v := range files {
		c.mapTasks[i].FileName = v
		c.mapTasks[i].FileId = i
		c.mapTasks[i].Status = 1
	}
	// go c.schedule()
	c.server()

	return &c
}
