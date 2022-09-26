package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

//貌似不需要请求参数，直接由coordinator决定分配map还是reduce
//整体逻辑就是map队列非空，分配map，map队列为空，分配reduce，reduce队列为空，返回finish，worker退出
type DispatchArgs struct {
	// state int8 //0-idle 1-map 2-reduce
}

//这里存在一种情况，就是map任务分配完了，但是还没执行完，所以不能执行reduce任务，这时候就需要wait
type DispatchReply struct {
	filename string
	taskType int8    //0-map 1-wait 2-reduce 3-finish
	nReduce  [2]int8 //reduce的任务号
}

type ReportArgs struct {
	tag int //0-map 1-reduce
}

type ReportRelpy struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
