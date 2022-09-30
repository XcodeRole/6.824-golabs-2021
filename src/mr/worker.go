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
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	nReduce int) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// id := GetID()
	for {
		reply := CallDispatch()
		switch reply.TaskType {
		case 0:
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			kva := mapf(reply.Filename, string(content))
			write2tmpfile(kva, reply.Filename, nReduce)
			ReportDone(reply.Filename)
		case 1:
			time.Sleep(time.Second * 3)
		case 2:
			files, err := ioutil.ReadDir(".")
			if err != nil {
				log.Fatal(err)
			}
			var kva []KeyValue
			reducesuffix := strconv.Itoa(reply.NoReduce)
			for _, fileinfo := range files {
				filename := fileinfo.Name()
				if strings.HasSuffix(filename, reducesuffix) {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatal(err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
			}
			//copy from sequential code
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + reducesuffix
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			ReportDone(reply.NoReduce)
		case 3:
			os.Exit(0)
		}

	}

}

func ReportDone(a interface{}) {
	args := ReportArgs{}
	reply := ReportReply{}
	switch a.(type) {
	case string:
		args.Tag = 0
		args.Filename = a.(string)
	case int:
		args.Tag = 1
		args.Reduceno = a.(int)
	}
	call("Coordinator.ReportDone", &args, &reply)
}

// func GetID() int {
// 	args := InitArgs{}
// 	reply := InitReply{}
// 	call("Coordinator.InitId", &args, &reply)
// 	return reply.Id
// }

func write2tmpfile(kva []KeyValue, maptaskfile string, nReduce int) {

	// workerid := strconv.Itoa(id)
	fileinfos, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	prefix := "mr-" + strconv.Itoa(ihash(maptaskfile)) + "-"
	//删除已经存在的
	for _, fileinfo := range fileinfos {
		if strings.HasPrefix(fileinfo.Name(), prefix) {
			os.Remove(fileinfo.Name())
		}
	}
	for _, kv := range kva {
		reduceid := strconv.Itoa(ihash(kv.Key) % nReduce)
		filename := prefix + reduceid
		// fmt.Println(filename)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("can't create middle file")
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallDispatch() DispatchReply {

	// declare an argument structure.
	args := DispatchArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := DispatchReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Dispatch", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
