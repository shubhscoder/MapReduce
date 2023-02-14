#pragma once

#include <string>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>

#define INTERMEDIATE_DIR "intermediate"
#define OUTPUT_DIR "output"

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		std::vector<std::pair<std::string,std::string>> entries_;
		uint64_t task_id_;
		uint64_t num_reducers_;
		std::vector<std::string> intermediate_files_;

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void init(uint64_t task_id, uint64_t num_reducers);



		std::vector<std::string> persistIntermediateData();
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	entries_.push_back(std::make_pair(key,val));
}

inline void BaseMapperInternal::init(uint64_t task_id, uint64_t num_reducers){
	entries_.clear();
	intermediate_files_.clear();
	task_id_ = task_id;
	num_reducers_ = num_reducers;
}

inline void initialiseOrClearDir(std::string dir_name){
	if (!std::filesystem::exists(std::filesystem::path(dir_name))){
		std::filesystem::create_directory(std::filesystem::path(dir_name));
	}
}

inline std::vector<std::shared_ptr<std::ofstream>> createIntermediateFilesForMap(uint64_t num, std::string prefix_string, std::vector<std::string>& intermediate_files){
	std::vector<std::shared_ptr<std::ofstream>> file_handles;
	for(int i = 0; i < num; i++){
		std::shared_ptr<std::ofstream> file_handle = std::make_shared<std::ofstream>();
		std::string file_path(std::string(INTERMEDIATE_DIR) + "/" + prefix_string + "_" + std::to_string(i));
		file_handle->open(file_path, std::ofstream::trunc);
		file_handles.push_back(file_handle);
		intermediate_files.push_back(file_path);
	}
	return file_handles;
}

// reference : https://stackoverflow.com/questions/8317508/hash-function-for-a-string
inline uint64_t hash(std::string str){
	uint64_t b = 378551;
    uint64_t a = 63689;
    uint64_t hash = 0;
    for(std::size_t i = 0; i < str.length(); i++)
    {
        hash = hash * a + str[i];
        a    = a * b;
    }
    return (hash & 0x7FFFFFFF);
}

inline std::vector<std::string> BaseMapperInternal::persistIntermediateData(){
	initialiseOrClearDir(INTERMEDIATE_DIR);
	std::vector<std::shared_ptr<std::ofstream>> file_handles = createIntermediateFilesForMap(num_reducers_, std::to_string(task_id_), intermediate_files_);
	for(auto e : entries_){
		std::string output_line = e.first + " " + e.second + "\n";
		file_handles[hash(e.first) % num_reducers_];
		file_handles[hash(e.first) % num_reducers_]->write(output_line.c_str(), output_line.size());
	}

	for(auto file_handle : file_handles){
		file_handle->close();
	}

	return intermediate_files_;
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::map<std::string, std::vector<std::string>> reducer_entries_; 
		std::map<std::string, std::string> final_reducer_output_; 
		uint64_t task_id_;
		std::string output_dir_;

		void init(uint64_t task_id, std::string output_dir);

		void addReducerEntries(std::string key, std::string value);

		std::map<std::string,std::vector<std::string>> getReducerEntries();

		void persistReducerOutput();

};

inline void BaseReducerInternal::init(uint64_t task_id, std::string output_dir) {

	reducer_entries_.clear();
	final_reducer_output_.clear();
	task_id_ = task_id;
	output_dir_ = output_dir;
}

inline void BaseReducerInternal::addReducerEntries(std::string key, std::string value){
	reducer_entries_[key].push_back(value);
}

inline std::map<std::string,std::vector<std::string>> BaseReducerInternal::getReducerEntries(){
	return reducer_entries_;
}

/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	final_reducer_output_[key] = val;
}

inline void BaseReducerInternal::persistReducerOutput() {

	initialiseOrClearDir(output_dir_);
	std::string output_file_path(std::string(output_dir_) + "/" + std::to_string(task_id_) + "_output");
	std::ofstream file_handle;
	file_handle.open(output_file_path, std::ofstream::trunc);

	int32_t fsize = final_reducer_output_.size();
	int32_t cnt = 0;

	for(auto e : final_reducer_output_){
		std::string output_line;
		if (cnt != fsize) {
			output_line = e.first + " " + e.second + "\n";
		} else {
			output_line = e.first + " " + e.second;
		}

		file_handle.write(output_line.c_str(), output_line.size());
		cnt++;
	}
	file_handle.close();
}