# CS 6210 Project4
MapReduce Infrastructure

# Overview

We have built a map reduce framework based on Google' map reduce paper (https://research.google.com/archive/mapreduce-osdi04.pdf). Mapreduce is a model for processing large amounts of data on commodity machines where hardware failure is commond and expected. This was originally implemented at Google by Jeff Dean and Sanjay Ghemawat. We implement map reduce to get an understanding of the framework and the programming paradigm. We have used grpc for communication between the master and the worker. Grpc is again a method of communication that was open sourced by Google and uses Protobuf for communication. This programming model is very popular because it abstracts the complexity of distributing computations across node. The user can just write the application logic (The map and the reduce functions) and the framework will process large amounts of data and get the output required by user. Popular applications of the map reduce include word counting in a large number of files, url visiting, finding minimum maximum temperatures, etc.

Following are the key differences between google's paper and our implementation.

1. In google's paper, the workers run of different machines altogether. There are thousands of machines (comodity hardware) where worker failure is very common. The master runs on a seperate machine and the failure of this machine is considered rare. In case the machine on which master is running fails, the user is expected to submit the entire mapreduce task again. In our implementation the workers and master run on the ***same machine*** as different processes.
2. The second difference is that in case of google's implementation the number of workers in not known beforehand. The workers can join in between computations. However, in our implementation, as per the expectation of the project, the config file provides the number of workers in advance (workers can however fail, which is handled).
3. The third difference is that google's map reduce implementation made use of the GFS or the google file system which is google proprietary distributed file system of google. In our implementation, we use the local file system of the single machine on which the master and the worker use.

# Implementation logic

# Master

1. The master acts as the client.
2. It receives the input as the number of files, and the other configs.
3. From the received input the master creates 'virtual shards' or logical partitions of the data across files.
4. We have written the sharding logic and that splits the shards in approximately the same size given by the config. We end the shard at the end of the line to avoid word being split in between.
5. The master is multithreaded and has two threads. One thread schedules the tasks from the task queue. The client on the master is asynchronous and schedules multiple tasks together.
6. The other thread monitors the completion queue. In case of successful responses for map, it will populate the appropriate reduce tasks. In case of reduce if the grpc returns with a ok sentence, then no work is required.

# Master Data structures.

1. A concurrent queue to handle the available workers: **available_workers_**
2. A concurrent queue to handle the available tasks: **tasks_**
3. Grpc CompletionQueue to handle the incoming responses for async tasks: **cq_**
4. An unordered map for caching the stubs. (Grpc documentation mentions caching stubs for better performance). **stubs_**
5. A vector to store the reduce tasks: **reduce_tasks_**

# Handling worker failures.

We check the response of the grpc runtime. If the response is ok, we handle it according to the task type. Else, we add that task back to the task queue for the scheduler to schedule the task.

# Handling slow workers.

We use the grpc deadline to take care of slow workers. We are using **10 seconds** as the deadline for the worker to respond. We think this is a reasonable limit as the communication is happening on the same machine. In real world use cases this response time should be set according to the following factors.
1. Load on server (Worker in this case).
2. Network bandwidth, etc.

We tested our code for slow workers by randomly adding sleep in the worker code. Our master recognized the slow worker, blacklisted it, and re-scheduled the tasks of the worker on other available workers.
Assumption for failures - Once master detects that a particular worker is slow or has crashed, it adds it to a blacklist and will not be used in future. We assume there are 1000s of workers available and hence doing so won't cause issues. 

# Worker implementation.

1. Worker is a stateless process that performs the task given by the master and can be of two types: map and reduce.
2. Worker class implements two services doMap and doReduce as rpc calls corresponding to each task type.
3. Worker also has some util functions like reading files in a given offset range and tokenizing strings based on a particular character.

# Map implementation
1. Map logic is implemented in BaseMapperInternal struct in mr_task.h file.
2. BaseMapperInternal stores task_id, no of reducers and intermediate buffer(BaseMapperInternal::entries_) data that will be written as an intermediate file after map task is finished.
3. Map task reads the file according to FileShard data structure and calls user-defined map function on each line.
4. Every map task creates R (no of reducers) intermediate files. We persist the buffered data based on a hashing function. For eg: An intermediate data entry (key1,value1) will be stored in
 hash(key1) % R th file.
5. All the intermediate file names are sent back to the master.

# Reduce Implementation
1. Reduce logic is implemented in ReducerMapperInternal struct in mr_task.h file.
2. Reduce task simply reads the given intermediate files and sorts them on keys. (We assume all keys fit in the memory). 
3. We process the read data using an ordered map that gives us 2 advantages. First it sorts all the entries by keys. Second, we can easily club same keys together to produce
 reduce (key, values[]) input.
4. This data (key, values[]) is then passed to user defined reduce function and the final output is stored in OUTPUT/ dir.

