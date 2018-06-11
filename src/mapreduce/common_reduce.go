package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	kvs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		readFile := reduceName(jobName, i, reduceTask)
		buf, err := ioutil.ReadFile(readFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "reduce read file err: %s\n", err)
		}
		var nkv KeyValues
		json.Unmarshal(buf, &nkv)
		kvs = append(kvs, nkv...)
	}
	sort.Sort(KeyValues(kvs))
	lenKvs := len(kvs)
	values := make([]string, 0)
	file, err := os.Create(outFile)
	defer file.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "reduce create file err: %s\n", err)
	}
	enc := json.NewEncoder(file)
	for i := 0; i < lenKvs; i++ {
		values = append(values, kvs[i].Value)
		if i == lenKvs-1 || kvs[i].Key != kvs[i+1].Key {
			enc.Encode(KeyValue{kvs[i].Key, reduceF(kvs[i].Key, values)})
			values = make([]string, 0)
		}
	}
}
