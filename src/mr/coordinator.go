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

type Mapqueue struct {
	q  []string
	mu sync.Mutex
}

func (mq *Mapqueue) isEmpty() bool {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if len(mq.q) == 0 {
		return true
	} else {
		return false
	}
}

func (mq *Mapqueue) Enqueue(str string) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.q = append(mq.q, str)
}

func (mq *Mapqueue) Dequeue() string {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	var file string = mq.q[0]
	mq.q = mq.q[1:len(mq.q)]
	return file
}

type Reducequeue struct {
	mu sync.Mutex
	q  []int
}

func (rq *Reducequeue) isEmpty() bool {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if len(rq.q) == 0 {
		return true
	} else {
		return false
	}
}

func (rq *Reducequeue) Enqueue(taskno int) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.q = append(rq.q, taskno)
}

func (rq *Reducequeue) Dequeue() int {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	var taskno int = rq.q[0]
	rq.q = rq.q[1:len(rq.q)]
	return taskno
}

type Coordinator struct {
	// Your definitions here.
	mq            Mapqueue
	rq            Reducequeue
	mapcounter    int
	reducecounter int
	idcounter     int
	cmu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Dispatch(args *DispatchArgs, reply *DispatchReply) error {
	if !c.mq.isEmpty() {
		reply.TaskType = 0
		reply.Filename = c.mq.Dequeue()
	} else if c.mapcounter != 0 {
		reply.TaskType = 1
	} else if !c.rq.isEmpty() {
		reply.TaskType = 2
		reply.NoReduce = c.rq.Dequeue()
	} else if c.reducecounter != 0 {
		/*
			这种情况是考虑到当前worker要等待其他worker执行完reduce。
			一旦其他worker执行的reduce任务失败了，还需要重新分配reduce
		*/
		reply.TaskType = 1
	} else {
		reply.TaskType = 3
	}
	return nil
}

//worker完成一个map或者reduce任务，向coordnator报告
func (c *Coordinator) ReportDone(args *ReportArgs, reply *ReportReply) error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	switch args.Tag {
	case 0:
		c.mapcounter -= 1
	case 1:
		c.reducecounter -= 1
	}
	return nil
}

//给worker分配id
func (c *Coordinator) InitId(args *InitArgs, reply *InitReply) error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	reply.Id = c.idcounter
	c.idcounter += 1
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	//map queue 和 reduce queue同时为空表示任务完成
	//但是这种方法是由bug的，比如最后一个map任务从队列中取出，但是这时候之前产生的reduce任务也被全部完成，那么就被认为是完成任务了，这时就出错了
	// if c.mq.isEmpty() && c.rq.isEmpty() {
	// 	ret = true
	// }
	//所以后续我维护了一个workcounter，每分配一个woker就加一，
	//但是这样需要worker运行完之后报告给coordinator,并且对这个数的操作要上锁

	//之前想着不用在map和reduce之间设置全局同步屏障，但是好像不太行，前面的想法都是基于map和reduce异步执行的，不太对，后续
	c.cmu.Lock()
	flag := c.reducecounter
	c.cmu.Unlock()
	if flag == 0 {
		time.Sleep(time.Second * 10)
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		mq: Mapqueue{
			q:  files,
			mu: sync.Mutex{},
		},
		rq: Reducequeue{
			q:  []int{},
			mu: sync.Mutex{},
		},
		mapcounter:    len(files),
		reducecounter: nReduce,
		idcounter:     0,
		cmu:           sync.Mutex{},
	}
	for i := 0; i < nReduce; i++ {
		c.rq.q = append(c.rq.q, i)
	}
	c.server()
	return &c
}

//好像不需要
// //触发器，当map任务执行结束之后将同一个reduce任务的文件合并
// func (c *Coordinator) trigger() {
// 	for c.mapcounter != 0 {
// 	}
// 	var buffer []string
// 	dir, err := ioutil.ReadDir("~/6.824/src/main")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	for _, fi := range dir {
// 		if strings.HasSuffix(fi.Name(), "mr-") {
// 			content := ioutil.ReadFile("~/6.824/src/main" + fi.Name())
// 			buffer = append(buffer, content)
// 		}
// 	}
// 	tmpfile, err := ioutil.TempFile("", "mr.*")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }
