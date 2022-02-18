package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
	"log"
	"net/rpc"
	"hash/fnv"
)

// KeyValue 键值对结构
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// 分区函数
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker Worker进程
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	JobA := JobArgs{WorkerID: 0}
	JobR := WorkerInfo{}
	for {
		call("Coordinator.AskTaskRpc", &JobA, &JobR)
		JobA.WorkerID = JobR.WorkerID
		switch JobR.State {
		case Waiting:
			time.Sleep(time.Second * 1)
			break
		case RunningMapTask:
			WorkMap(mapf, JobR)
			break
		case RunningReduceTask:
			WorkReduce(reducef, JobR)
			break
		case Exit:
			time.Sleep(time.Second * 1)
			return
		}

	}
}

// WorkMap Worker执行Map流程
func WorkMap(mapf func(string, string) []KeyValue, job WorkerInfo) error {
	if job.RunTask.TaskType != MapTsk {
		return errors.New("TaskType Error")
	}
	oNamePre := "mr-" + strconv.Itoa(job.RunTask.MapIndex) + "-"
	rawfile, err := os.Open(job.RunTask.MapTaskFile)
	if err != nil {
		log.Fatalf("can't open %v", job.RunTask.MapFilename)
	}
	content, err := ioutil.ReadAll(rawfile)
	if err != nil {
		log.Fatalf("can't read %v", job.RunTask.MapFilename)
	}
	rawfile.Close()
	kva := mapf(job.RunTask.MapTaskFile, string(content))
	sort.Sort(ByKey(kva))
	// 输出temp文件
	intermediateFiles := make([]*os.File, job.RunTask.ReduceNumber)
	fileEnc := make([]*json.Encoder, job.RunTask.ReduceNumber)
	for outIndex := 0; outIndex < job.RunTask.ReduceNumber;outIndex++ {
		intermediateFiles[outIndex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEnc[outIndex] = json.NewEncoder(intermediateFiles[outIndex])
	}
	for _, kv := range kva {
		toFileIndex := ihash(kv.Key) % job.RunTask.ReduceNumber
		enc := fileEnc[toFileIndex]
		err := enc.Encode(&kv)
		if err != nil {
			panic("json write Error")
		}
	}
	for rIndex, file := range intermediateFiles {
		oName := oNamePre + strconv.Itoa(rIndex)
		oldPath := filepath.Join(file.Name())
		os.Rename(oldPath, oName)
		file.Close()
		job.RunTask.MapFilename[rIndex] = oName
	}
	call("Coordinator.AskTaskRpc", &job, &job)
	return nil
}

// WorkReduce Worker执行Reduce流程
func WorkReduce(reducef func(string, []string) string, job WorkerInfo) error {
	outFileName := "mr-out-" + strconv.Itoa(job.RunTask.ReduceIndex)
	var intermediate []KeyValue
	for _, filename := range job.RunTask.ReduceFilename {
		file, err := os.Open(filename)
		if err != nil {
			panic("can't open file")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile("mr-tmp", "mr-tmp-*")
	if err != nil {
		panic("create tempFile error")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outFileName)
	ofile.Close()
	job.RunTask.oFilename = outFileName
	call("Coordinator.AskTaskRpc", &job, &job)
	return nil
}


// CallRPC函数
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
