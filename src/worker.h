#pragma once
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <unistd.h>
#include "masterworker.grpc.pb.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::MapReduce;
using masterworker::MapTaskResult;
using masterworker::MapTaskArg;
using masterworker::ReduceTaskResult;
using masterworker::ReduceTaskArg;
using masterworker::ShardInfo;


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);


/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker final : public MapReduce::Service{

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		void processFileForMap(ShardInfo f, std::shared_ptr<BaseMapper> mapper){
			std::ifstream fin(f.filename());
			fin.seekg(f.start_offset());
			int offset = 0;
			while(!fin.eof() && offset < f.length() ){
				std::string curr_line;
				std::getline(fin, curr_line);
				offset += curr_line.size();
				offset++;
				mapper->map(curr_line);
			}
		}

		// This is the Map RPC handler on the worker side. 
		Status doMap(ServerContext* context, const MapTaskArg* request, MapTaskResult* reply) override {
			int32_t numi = rand() % 100;

			auto mapper = ::get_mapper_from_task_factory(request->user_id());
			int num_of_files = request->shard_info_size();
			
			mapper->impl_->init(request->task_id(), request->n_reducers());
			for(int i = 0; i < num_of_files; i++){
				ShardInfo file = request->shard_info(i);
				processFileForMap(file, mapper);
			}

			std::vector<std::string> intermediate_files = mapper->impl_->persistIntermediateData();
			int i = 0;
			for(std::string file : intermediate_files){
				std::string* str_reply = reply->add_files();
				*str_reply = file;
			}
			return Status::OK;
		}


		std::vector<std::string> tokenize(std::string input, std::string delimiter = " "){
			std::vector<std::string> result;
			char *token = std::strtok(const_cast<char*>(input.c_str()), delimiter.c_str());
			while (token != nullptr)
			{
				result.push_back(std::string(token));
				token = strtok(nullptr, delimiter.c_str());
			}
			return result;
		}

		void processIntermediateFileForReduce(std::string file_name, std::shared_ptr<BaseReducer> reducer){
			std::ifstream fin(file_name);
			while(!fin.eof()){
				std::string curr_line;
				std::getline(fin, curr_line);
				std::vector<std::string> tokenized_line = tokenize(curr_line);
				if(tokenized_line.size() == 2){
					reducer->impl_->addReducerEntries(tokenized_line[0], tokenized_line[1]);
				}
			}
		}

		// This is the Reduce RPC handler on the worker side. 
		Status doReduce(ServerContext* context, const ReduceTaskArg* request, ReduceTaskResult* reply) override {
			auto reducer = ::get_reducer_from_task_factory(request->user_id());
			reducer->impl_->init(request->task_id(), request->output_dir());
			int num_of_files = request->files_size();
			for(int i = 0; i < num_of_files; i++){
				std::string file_name = request->files(i);
				processIntermediateFileForReduce(file_name, reducer);
			}

			std::map<std::string,std::vector<std::string>> reducer_entries = reducer->impl_->getReducerEntries();
			for(auto reducer_entry : reducer_entries){
				reducer->reduce(reducer_entry.first, reducer_entry.second);
			}
			reducer->impl_->persistReducerOutput();
			return Status::OK;
		}

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr_port_;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	this->ip_addr_port_ = ip_addr_port;
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	srand(time(0));
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	ServerBuilder worker_builder_;
	worker_builder_.AddListeningPort(this->ip_addr_port_, grpc::InsecureServerCredentials());
  	worker_builder_.RegisterService(this);
	std::unique_ptr<Server> server(worker_builder_.BuildAndStart());
	server -> Wait();
	return true;
}