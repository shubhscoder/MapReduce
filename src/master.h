#ifndef MASTER_H
#define MASTER_H

#include <atomic>
#include <thread>
#include <memory>
#include <string>
#include <iostream>
#include "mapreduce_spec.h"
#include "concurrent_queue.h"
#include "file_shard.h"
#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include <chrono>
#include <unordered_set>

#include "masterworker.grpc.pb.h"

using std::thread;
using std::shared_ptr;
using std::unique_ptr;
using std::make_shared;
using std::make_unique;
using std::unordered_map;
using std::to_string;
using std::unordered_set;
using std::chrono::system_clock;
using std::chrono::seconds;
using std::atomic_int32_t;
using std::atomic_int64_t;
using std::atomic_bool;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using masterworker::MapReduce;
using masterworker::MapTaskArg;
using masterworker::MapTaskResult;
using masterworker::ReduceTaskArg;
using masterworker::ReduceTaskResult;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

// A data structure to store all information related to a task.
struct TaskStatus {

	enum TaskType {
		MapTask,
		ReduceTask
	};

	// Type of the task.
	TaskType type_;

	enum TaskState {
		// A task is in this state if it is created, and still waiting in the task queue for its chance.
		NotStarted,

		// A task that has been admitted by master and send to a corresponding worker.
		Running,

		// A task that has failed, either because of worker failure or transport error.
		Failed,

		// A task that as completed successfully.
		Succeeded
	};

	// The current state of the task.
	TaskState status_;

	// Worker on which the task is running. Applicable only if status_ is running.
	string worker_;

	// Start time of the task. Applicable only for a running task.
	system_clock::time_point start_time_;

	// The shard that is handled by this task. Applicable only for map tasks.
	shared_ptr<FileShard> shard_;

	TaskStatus() {
		// By default a task is in the NotStarted stage.
		status_ = TaskStatus::NotStarted;
	}
};

// Encapsulates the work to be done in the map phase.
struct Task {
	// A unique identifier for the task.
	int64_t task_id_;

	// The shard which this task is using.
	shared_ptr<FileShard> shard_;
};


struct WorkerClient {

	// Context for the client, It can be used to send some extra information 
	// to the server and alter some configurations of the RPC.
	unique_ptr<ClientContext> context_;
	
	// Container for the data that is expected as response to the map rpc.
	unique_ptr<MapTaskResult> map_reply_;

	// Container for the data that is expected as response to the reduce rpc.
	unique_ptr<ReduceTaskResult> reduce_reply_;

	// We are making an asynchronous rpc call to the worker. We use this to finish the rpc
	// and wait for the response of the map rpcs. 
	unique_ptr<ClientAsyncResponseReader<MapTaskResult> > rpc_;

	// Same as above, just that we wait for the response of the reduce rpc.
	unique_ptr<ClientAsyncResponseReader<ReduceTaskResult> > reduce_rpc_;

	// Id of the task, that is being handled by this particular worker.
	int32_t task_id_;

	// Address of the worker.
	string worker_;

	WorkerClient() {
		context_ = make_unique<ClientContext> ();

		status_ = make_unique<Status> ();

		map_reply_ = make_unique<MapTaskResult> ();

		reduce_reply_ = make_unique<ReduceTaskResult> ();
	}
};
 
class Master {

	// When done_ is set to true, the map reduce operation is complete 
	// and the master is free to exit.
	bool done_;

	/* 
	   A thread-safe queue to store the list of workers available.
	   In the original map reduce paper, worker discovery happened when 
	   the worker sent a request to the master. However, in this project, the
	   requirement was modified and the list of available workers is already
	   known to the master.
	*/
	ConcurrentQueue<string> available_workers_;

	// 'Work' that is to be done. During the map phase, this will hold the map tasks,
	// and during the reduce phase this will hold the reduce tasks.
	ConcurrentQueue<Task> tasks_;

	// A map to store the status and metadata related to each task.
	unordered_map<int64_t, TaskStatus> task_status_;

	// A mutex to protect the above map, in case of multiple threads
	// accessing the map.
	mutex task_status_mt_;

	// Number of reduce tasks, aka number of reduce workers.
	int64_t n_reduce_tasks_;

	// Number of map tasks, which is equal to the number of shards.
	int32_t n_map_tasks_;

	// Directory where the output of reduce tasks will be saved.
	string output_dir_;

	// Completion queue, where the response to the async tasks are stored.
	// We need to poll on this queue in a seperate thread and check responses to our tasks.
	CompletionQueue cq_;

	// Number of map tasks that are completed.
	atomic_int32_t map_cnt_;

	// For the map phase, this is equal to number of map tasks,
	// For the reduce phase, this is equal to number of reduce tasks.
	int32_t n_task_end_;

	// A cache for the worker stubs. Creating a new stub everytime is very inefficient.
	unordered_map<string,  unique_ptr<MapReduce::Stub> > stubs_;

	// Done map is set to true at the end of the map phase.
	atomic_bool done_map_;

	// The reduce task stores results of the each map task.
	// The input to ith reduce task is reduce_tasks_[j][i] where j is from 0..n 
	vector<vector<string> > reduce_tasks_;

	// This user id is used by the worker to invoke the specific implementation of the
	// map and reduce tasks.
	string user_id_;

	// A counter to generate unique ids for map and reduce tasks.
	atomic_int64_t task_iter_;

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:

		// The master schedules the map tasks.
		void SendMapTasks();

		// The master schedules the reduce tasks.
		void SendReduceTasks();

		// Function that polls on the completion queue to handle responses of the
		// asynchronous rpc's sent to the workers.
		void HandleResponses();

		// Function to get a unique id for each task.
		int64_t get_id();
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : done_(false), n_reduce_tasks_(mr_spec.n_outputs_), done_map_(false), map_cnt_(0), task_iter_(0) {
	for (string worker: mr_spec.worker_addr_) {
		available_workers_.Push(worker);
		stubs_[worker] = MapReduce::NewStub(grpc::CreateChannel(worker, grpc::InsecureChannelCredentials()));
	}

	user_id_ = mr_spec.user_id_;
	output_dir_ = mr_spec.output_dir_;

	n_map_tasks_ = file_shards.size();
	done_map_ = false;

	for (int i = 0; i < file_shards.size(); i++) {
		Task cur;
		cur.task_id_ = get_id();
		cur.shard_ = make_shared<FileShard> (file_shards[i]);
		tasks_.Push(cur);
	}

	n_task_end_ = n_map_tasks_;

	reduce_tasks_ = vector<vector<string> > (n_reduce_tasks_);
}

int64_t Master::get_id() {
	return task_iter_++;
}

/*
The algorithm for this function is as follows:
	1. Get an available taskk, if we don't get a task poll again.
	2. Once we get a task, retrieve the available worker. If no worker is available, poll again
	but, do not, override the already acquired task.
	3. It is necessary that the queue does not wait undefinitely until a or
	tasks is available. This would result in a deadlock, at the end, when all tasks
	are done.
*/
void Master::SendMapTasks() {
	int32_t cnt = 0;
	shared_ptr<Task> cur_task_ptr = nullptr;
	shared_ptr<string> worker_ptr = nullptr;

	while (done_map_ == false) {
		// Get available worker
		if (cur_task_ptr == nullptr) {
			cur_task_ptr = tasks_.wait_and_pop();
		}

		if (cur_task_ptr == nullptr) {
			// come later and check again
			continue;
		}

		if (worker_ptr == nullptr) {
			worker_ptr = available_workers_.wait_and_pop();
		}

		if (worker_ptr == nullptr) {
			continue;
		}

		Task cur_task = *cur_task_ptr;
		string worker = *worker_ptr;
		cur_task_ptr = nullptr;
		worker_ptr = nullptr;

		// The deadline for the rpc is 10 secs (Requirement from the project).
		// Let's be a little conservative and set the deadline to 9 secs to avoid delays
		// because of race conditions, etc.
		system_clock::time_point deadline = system_clock::now() + seconds(9);
		WorkerClient* client = new WorkerClient();
		client->context_->set_deadline(deadline);
		
		// Populate request for the map task.
		MapTaskArg request;
		MapTaskResult result;
		int32_t task_id = cur_task.task_id_;
		request.set_task_id(task_id);
		request.set_dir_path(output_dir_);
		request.set_n_reducers(n_reduce_tasks_);
		request.set_user_id(user_id_);
		for (auto& cur: cur_task.shard_->file_info_list_) {
			auto* shard_info = request.add_shard_info();
			shard_info->set_filename(cur.file_name_);
			shard_info->set_start_offset(cur.file_start_pos_);
			shard_info->set_length(cur.shard_length_);
		}

		// Do the necessary book-keeping for sending this task.
		TaskStatus cur_status;
		cur_status.status_ = TaskStatus::Running;
		cur_status.type_ = TaskStatus::MapTask;
		cur_status.worker_ = worker;
		cur_status.shard_ = cur_task.shard_;
		cur_status.start_time_ = system_clock::now();
		{
			unique_lock<mutex> mt(task_status_mt_);
			task_status_[task_id] = cur_status;
		}

		client->task_id_ = task_id;
		client->worker_ = worker;
		client->rpc_ = stubs_[worker]->AsyncdoMap(client->context_.get(), request, &cq_);
		client->rpc_->Finish(client->map_reply_.get(), client->status_.get(), (void*) client);
	}
}

/*
The algorithm is similar to the map phase.

TODO(Shubham): Deduplicate code in the SendMapTasks and SendReduceTasks.
*/
void Master::SendReduceTasks() {
	shared_ptr<Task> cur_task_ptr = nullptr;
	shared_ptr<string> worker_ptr = nullptr;

	while (done_map_ == false) {
		// Get available worker
		if (cur_task_ptr == nullptr) {
			cur_task_ptr = tasks_.wait_and_pop();
		}

		if (cur_task_ptr == nullptr) {
			// come later and check again
			continue;
		}

		if (worker_ptr == nullptr) {
			worker_ptr = available_workers_.wait_and_pop();
		}

		if (worker_ptr == nullptr) {
			continue;
		}

		Task cur_task = *cur_task_ptr;
		string worker = *worker_ptr;
		cur_task_ptr = nullptr;
		worker_ptr = nullptr;

		system_clock::time_point deadline = system_clock::now() + seconds(9);
		WorkerClient* client = new WorkerClient();

		client->context_->set_deadline(deadline);
		ReduceTaskArg request;
		ReduceTaskResult result;
		
		int32_t task_id = cur_task.task_id_;

		request.set_task_id(task_id);
		request.set_user_id(user_id_);
		request.set_output_dir(output_dir_);

		for (string& file_name: reduce_tasks_[task_id]) {
			*request.add_files() = file_name;
		}

		TaskStatus cur_status;
		cur_status.status_ = TaskStatus::Running;
		cur_status.type_ = TaskStatus::ReduceTask;
		cur_status.worker_ = worker;
		cur_status.start_time_ = system_clock::now();
		{
			unique_lock<mutex> mt(task_status_mt_);
			task_status_[task_id] = cur_status;
		}

		client->task_id_ = task_id;
		client->worker_ = worker;
		client->reduce_rpc_ = stubs_[worker]->AsyncdoReduce(client->context_.get(), request, &cq_);
		client->reduce_rpc_->Finish(client->reduce_reply_.get(), client->status_.get(), (void*) client);
	}
}

void Master::HandleResponses() {
	while (!done_) {
		void* which_tag;
		bool ok = false;

		system_clock::time_point deadline = system_clock::now() + std::chrono::milliseconds(200);

		auto status = cq_.AsyncNext(&which_tag, &ok, deadline);

		if (status == grpc::CompletionQueue::TIMEOUT) {
			// check if we are done and wait again.
			continue;
		}

		WorkerClient* rpc_tag = static_cast<WorkerClient*>(which_tag);

		TaskStatus::TaskType task_type = TaskStatus::MapTask;
		{
			unique_lock<mutex> lk(task_status_mt_);
			task_type = task_status_[rpc_tag->task_id_].type_;
		}


		if (rpc_tag->status_->ok()) {

			if (task_type == TaskStatus::MapTask) {
				{
					int cnt = 0;

					for (auto iter: rpc_tag->map_reply_->files()) {
						reduce_tasks_[cnt].push_back(iter);
						cnt++;
					}
				}
			}

			{
				unique_lock<mutex> lk(task_status_mt_);
				task_status_[rpc_tag->task_id_].status_ = TaskStatus::Succeeded;
			}

			available_workers_.Push(rpc_tag->worker_);
			map_cnt_++;
			if (map_cnt_ == n_task_end_) {
				done_map_ = true;
			}
		} else {
				// This task needs to be rescheduled.
				// The worker needs to be ignored.
				Task new_task;
				new_task.task_id_ = get_id();
				if (task_type == TaskStatus::MapTask) {
					unique_lock<mutex> lk(task_status_mt_);
					new_task.shard_ = task_status_[rpc_tag->task_id_].shard_;
				} else {
					cout << "Reduce task gone wrong " << new_task.task_id_ << endl;
				}
				tasks_.Push(new_task);
		}

		delete rpc_tag;
	}
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	thread map_scheduler(&Master::SendMapTasks, this);
	thread response_handle(&Master::HandleResponses, this);

	map_scheduler.join();

	done_map_ = false;

	{
		unique_lock<mutex> lk(task_status_mt_);
		task_status_.clear();
	}

	n_task_end_ = n_reduce_tasks_;
	done_map_ = false;
	map_cnt_ = 0;

	for (int32_t i = 0; i < n_reduce_tasks_; i++) {
		Task cur;
		cur.task_id_ = i;
		tasks_.Push(cur);
	}
	
	thread reduce_scheduler(&Master::SendReduceTasks, this);
	reduce_scheduler.join();

	done_ = true;
	response_handle.join();

	return done_;
}

#endif