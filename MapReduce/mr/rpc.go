package mr

// RPC参数

import (
	"os"
	"strconv"
)

// JobArgs AskTaskRPC参数
type JobArgs struct {
	WorkerID	int
}

// 协调者的socket文件
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
