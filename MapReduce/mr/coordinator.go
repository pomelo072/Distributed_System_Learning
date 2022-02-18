package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


// 任务类型
const (
	MapTsk = RunningMapTask
	ReduceTsk = RunningReduceTask
)

// 运行状态
const (
	Waiting = iota
	RunningMapTask
	RunningReduceTask
	Exit
)

// Coordinator 协调者的定义
type Coordinator struct {
	CLock			sync.Mutex
	// 任务阶段
	JobPhase		int
	// Worker信息
	Workers     	map[int]WorkerInfo
	// MapTask队列
	MapTasks    	MapTask
	// ReduceTask队列
	ReduceTasks 	ReduceTask
}

// MapTask MapTask队列
type MapTask struct {
	// MapTask任务数
	MapNumber  		int
	// 等待任务
	WaitedTask 		chan Task
	// 运行中任务
	RunningTask		map[int]Task
	// 完成任务
	FinishedTask	[]Task
}

// ReduceTask ReduceTask队列
type ReduceTask struct {
	// ReduceTask任务数
	ReduceNumber 	int
	// 等待任务
	WaitedTask 		chan Task
	// 运行中任务
	RunningTask		map[int]Task
	// 完成任务
	FinishedTask	[]Task
}

// WorkerInfo Worker信息
type WorkerInfo struct {
	WorkerID		int
	// Worker状态
	State        int
	// 正在运行任务
	RunTask      Task
	ReduceNumber int
}

// Task 任务信息
type Task struct {
	//TaskID
	TaskID			int
	// Task类型 M/R
	TaskType		int
	// 开始时间
	BeginTime 		time.Time
	// MapTask 的执行文件
	MapTaskFile 	string
	// Map产生的中间文件
	MapFilename		map[int]string
	// Map任务编号
	MapIndex    	int
	// Reduce任务编号
	ReduceIndex		int
	// Reduce任务数量
	ReduceNumber	int
	// ReduceTask 的执行文件
	ReduceFilename	[]string
	// 最终输出文件
	oFilename		string
}

// RPC调用集

// AskTaskRpc 任务查询RPC
func (c *Coordinator) AskTaskRpc(args *JobArgs, reply *WorkerInfo) error {
	// Worker检索
	c.CLock.Lock()
	defer c.CLock.Unlock()
	if args.WorkerID == 0 {
		w := len(c.Workers)+1
		reply = &WorkerInfo{WorkerID: w}
		c.Workers[w] = *reply
	} else {
		*reply = c.Workers[args.WorkerID]
	}

	// 是否Exit阶段
	if c.JobPhase == Exit {
		reply.State = Exit
		delete(c.Workers, reply.WorkerID)
		return nil
	}
	// 进入Map阶段
	if c.JobPhase == RunningMapTask && len(c.MapTasks.WaitedTask) > 0{
		reply.State = RunningMapTask
		reply.RunTask = <- c.MapTasks.WaitedTask
		c.MapTasks.RunningTask[reply.RunTask.TaskID] = reply.RunTask
		reply.RunTask.BeginTime = time.Now()
		return nil
	}
	// 进入Reduce阶段
	if c.JobPhase == RunningReduceTask && len(c.ReduceTasks.WaitedTask) > 0 {
		reply.State = RunningReduceTask
		reply.RunTask = <- c.ReduceTasks.WaitedTask
		c.ReduceTasks.RunningTask[reply.RunTask.TaskID] = reply.RunTask
		reply.RunTask.BeginTime = time.Now()
		return nil
	}
	// Waiting 等待可能分配任务
	if len(c.MapTasks.RunningTask) > 0 || len(c.ReduceTasks.RunningTask) > 0 {
		reply.State = Waiting
		return nil
	}
	return nil
}

// TaskDoneRpc 任务完成检查RPC
func (c *Coordinator) TaskDoneRpc(args *WorkerInfo, reply *WorkerInfo) error {
	c.CLock.Lock()
	defer c.CLock.Unlock()
	*reply = c.Workers[args.WorkerID]
	switch reply.State {
	case RunningMapTask:
		c.MapTasks.FinishedTask = append(c.MapTasks.FinishedTask, reply.RunTask)
		delete(c.MapTasks.RunningTask, reply.RunTask.TaskID)
		// 所有Map完成
		if len(c.MapTasks.FinishedTask) == c.MapTasks.MapNumber {
			c.LoadReduceFiles()
			c.JobPhase = RunningReduceTask
		}
		break
	case RunningReduceTask:
		c.ReduceTasks.FinishedTask = append(c.ReduceTasks.FinishedTask, reply.RunTask)
		delete(c.ReduceTasks.RunningTask, reply.RunTask.TaskID)
		// 所有Reduce完成
		if len(c.ReduceTasks.FinishedTask) == c.ReduceTasks.ReduceNumber {
			c.JobPhase = Exit
		}
		break
	default:
		panic("TaskDone Error")
	}
	return nil
}

// LoadReduceFiles 读取中间文件生成Reduce任务
func (c *Coordinator) LoadReduceFiles() {
	var ReduceFile map[int][]string
	for _, task := range c.MapTasks.FinishedTask {
		for idx, fileset := range task.MapFilename{
			ReduceFile[idx] = append(ReduceFile[idx], fileset)
		}
	}
	for i := 0; i < c.ReduceTasks.ReduceNumber; i++ {
		task := Task{
			TaskID:         i,
			TaskType:       ReduceTsk,
			ReduceIndex:    i,
			ReduceNumber:   c.ReduceTasks.ReduceNumber,
			ReduceFilename: ReduceFile[i],
		}
		c.ReduceTasks.WaitedTask <- task
	}
}

// 任务超时容错机制

// IsTimeout 检查任务是否超时
func (w *WorkerInfo) IsTimeout() bool {
	return time.Now().Sub(w.RunTask.BeginTime) > time.Second * 10
}

// TimeOut 终止超时任务
func (c *Coordinator) TimeOut()  {
	for _, info := range c.Workers {
		if info.IsTimeout() {
			c.CLock.Lock()
			switch info.RunTask.TaskType {
			case MapTsk:
				c.MapTasks.WaitedTask <- info.RunTask
				delete(c.MapTasks.RunningTask, info.RunTask.TaskID)
			case ReduceTsk:
				c.ReduceTasks.WaitedTask <- info.RunTask
				delete(c.ReduceTasks.RunningTask, info.RunTask.TaskID)
			}
			info.State = Waiting
			c.CLock.Unlock()
		}
	}
}

// CollectionTimeOut 收集超时任务
func (c *Coordinator) CollectionTimeOut() {
	for  {
		time.Sleep(time.Second * 5)
		c.TimeOut()
	}
}

// 协调者监听Worker端口
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

// Done 协调者完成整个工作流程
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if len(c.MapTasks.RunningTask) == 0 && len(c.ReduceTasks.RunningTask) == 0 && c.JobPhase == ReduceTsk {
		c.JobPhase = Exit
	}
	if len(c.Workers) == 0 && c.JobPhase == Exit {
		ret = true
	}
	return ret
}

// MakeCoordinator 生成协调者
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{JobPhase: Waiting}
	c.ReduceTasks.ReduceNumber = nReduce
	c.MapTasks.MapNumber = len(files)
	for fileIndex, filename := range files {
		t := Task{
			TaskID:       fileIndex,
			TaskType:     MapTsk,
			MapIndex:     fileIndex,
			MapTaskFile:  filename,
			ReduceNumber: nReduce,
		}
		c.MapTasks.WaitedTask <- t
	}
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err := os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			panic("create tmp folder error")
		}
	}
	c.JobPhase = RunningMapTask
	// 检查超时线程
	go c.CollectionTimeOut()
	c.server()
	return &c
}
