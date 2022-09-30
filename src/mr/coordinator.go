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

//任务类型
type maptask string
type reducetask int

type Mapqueue struct {
	q  []maptask
	mu sync.Mutex
}

//内部带锁的reducetask的队列，后续的结构体函数都是sync的
type Reducequeue struct {
	q  []reducetask
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

func (mq *Mapqueue) Enqueue(task maptask) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.q = append(mq.q, task)
}

func (mq *Mapqueue) Dequeue() maptask {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	task := mq.q[0]
	mq.q = mq.q[1:len(mq.q)]
	return task
}

// func (mq *Mapqueue) Outbyvalue(delno string) maptask {
// 	// mq.mu.Lock()
// 	// defer mq.mu.Unlock()
// 	var task maptask
// 	for i := 0; i < len(mq.q); i++ {
// 		if mq.q[i].filename == delno {
// 			task = mq.q[i]
// 			mq.q = append(mq.q[:i], mq.q[i+1:]...)
// 			break
// 		}
// 	}
// 	return task
// }

func (rq *Reducequeue) isEmpty() bool {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if len(rq.q) == 0 {
		return true
	} else {
		return false
	}
}

func (rq *Reducequeue) Enqueue(task reducetask) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.q = append(rq.q, task)
}

func (rq *Reducequeue) Dequeue() reducetask {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	task := rq.q[0]
	rq.q = rq.q[1:len(rq.q)]
	return task
}

// func (rq *Reducequeue) Outbyvalue(delno int) reducetask {
// 	// rq.mu.Lock()
// 	// defer rq.mu.Unlock()
// 	var task reducetask
// 	for i := 0; i < len(rq.q); i++ {
// 		if rq.q[i].reduceno == delno {
// 			task = rq.q[i]
// 			rq.q = append(rq.q[:i], rq.q[i+1:]...)
// 			break
// 		}
// 	}
// 	return task
// }

//正在运行的任务队列，用map实现，同样内部带锁，后续的操作都是sync的
type runmapmanager struct {
	runlist map[maptask]int
	mu      sync.Mutex
}

type runreducemanager struct {
	runlist map[reducetask]int //value - counter , 用来判断是否超时
	mu      sync.Mutex
}

func (r *runmapmanager) isEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runlist) == 0
}
func (r *runreducemanager) isEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runlist) == 0
}

func (r *runmapmanager) syncenmap(task maptask, counter int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runlist[task] = counter
}
func (r *runreducemanager) syncenmap(task reducetask, counter int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runlist[task] = counter
}

//由于for循环遍历也要加锁，我不知该咋解决，所以把del改为不加锁的，在业务代码中显式加锁
func (r *runmapmanager) del(task maptask) {
	delete(r.runlist, task)
}
func (r *runreducemanager) del(task reducetask) {
	delete(r.runlist, task)
}

//时间加1,异步的
func (r *runmapmanager) timeplus() {
	for k := range r.runlist {
		r.runlist[k]++
	}
}
func (r *runreducemanager) timeplus() {
	for k := range r.runlist {
		r.runlist[k]++
	}
}

/*
	coordinator维护四个队列，waitformap、runmap、waitforreduce、runreduce
	这样可以处理超时任务，比如某个任务crash之后，他的counter就会大于某个阈值，然后重新把任务放到wait队列中
*/
type Coordinator struct {
	// Your definitions here.
	// mapcounter    int
	// reducecounter int
	// idcounter     int
	// 不用counter，四个队列来管理
	waitformap    Mapqueue
	runmap        runmapmanager
	waitforreduce Reducequeue
	runreduce     runreducemanager
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Dispatch(args *DispatchArgs, reply *DispatchReply) error {
	if !c.waitformap.isEmpty() {
		reply.TaskType = 0
		task := c.waitformap.Dequeue()
		c.runmap.syncenmap(task, 0)
		reply.Filename = string(task)
	} else if !c.runmap.isEmpty() { //wait util all map task done
		reply.TaskType = 1
	} else if !c.waitforreduce.isEmpty() {
		reply.TaskType = 2
		task := c.waitforreduce.Dequeue()
		c.runreduce.syncenmap(task, 0)
		reply.NoReduce = int(task)
	} else if !c.runreduce.isEmpty() {
		/*
			这种情况是考虑到当前worker要等待其他worker执行完reduce。
			一旦其他worker执行的reduce任务失败了，还需要重新分配reduce
			和上面的wait map task作用一样，只不过这个是等reduce task
		*/
		reply.TaskType = 1
	} else {
		reply.TaskType = 3
	}
	return nil
}

//worker完成一个map或者reduce任务，向coordnator报告
func (c *Coordinator) ReportDone(args *ReportArgs, reply *ReportReply) error {
	switch args.Tag {
	case 0:
		c.runmap.mu.Lock()
		c.runmap.del(maptask(args.Filename))
		c.runmap.mu.Unlock()
	case 1:
		c.runreduce.mu.Lock()
		c.runreduce.del(reducetask(args.Reduceno))
		c.runreduce.mu.Unlock()
	}
	return nil
}

//coordinator开一个协程来对正在运行的任务进行计时
func (c *Coordinator) timemanager() {
	for {
		time.Sleep(time.Second)
		if !c.runmap.isEmpty() {
			c.runmap.mu.Lock()
			c.runmap.timeplus()
			for k, v := range c.runmap.runlist {
				if v > 10 {
					c.waitformap.Enqueue(k)
					c.runmap.del(k)
				}
			}
			c.runmap.mu.Unlock()
		}
		if !c.runreduce.isEmpty() {
			c.runreduce.mu.Lock()
			c.runreduce.timeplus()
			for k, v := range c.runreduce.runlist {
				if v > 10 {
					c.waitforreduce.Enqueue(k)
					c.runreduce.del(k)
				}
			}
			c.runreduce.mu.Unlock()
		}
		//主进程会每秒进行判断，所以这里不需要了，主进程退出，协程也会退出
		// if c.Done() {
		// 	os.Exit(0)
		// }
	}
}

//现在不需要worker id，中间文件直接用任务的filename名命名，这样的坏处就是会产生很多中间文件，
//如果一个输出文件包括多个map任务的话，一个worker down掉之后就不能将他们区分开！

// //给worker分配id
// func (c *Coordinator) InitId(args *InitArgs, reply *InitReply) error {
// 	c.cmu.Lock()
// 	defer c.cmu.Unlock()
// 	reply.Id = c.idcounter
// 	c.idcounter += 1
// 	return nil
// }

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
	// c.cmu.Lock()
	// flag := c.reducecounter
	// c.cmu.Unlock()
	// if flag == 0 {
	// 	time.Sleep(time.Second * 10)
	// 	ret = true
	// }

	//四个队列全空，结束
	if c.waitformap.isEmpty() && c.runmap.isEmpty() && c.waitforreduce.isEmpty() && c.runreduce.isEmpty() {
		ret = true
		//等待两秒，给worker返回结束信息
		time.Sleep(time.Second * 2)
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
	waitformap := Mapqueue{mu: sync.Mutex{}}
	runmap := runmapmanager{mu: sync.Mutex{}, runlist: map[maptask]int{}}
	waitforreduce := Reducequeue{mu: sync.Mutex{}}
	runreduce := runreducemanager{mu: sync.Mutex{}, runlist: map[reducetask]int{}}
	for _, file := range files {
		waitformap.Enqueue(maptask(file))
	}
	for i := 0; i < nReduce; i++ {
		waitforreduce.Enqueue(reducetask(i))
	}
	c := Coordinator{
		waitformap:    waitformap,
		runmap:        runmap,
		waitforreduce: waitforreduce,
		runreduce:     runreduce,
	}
	go c.timemanager()
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
