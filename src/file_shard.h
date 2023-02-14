#pragma once

#include <vector>
#include<iostream>
#include <fstream>
#include "mapreduce_spec.h"

using std::string;
using std::vector;
using std::ifstream;

struct FileInfo {
     string file_name_;

     // Specifies the starting position of the file in a particular shard.
     // A shard can consist of various files, and various offsets of a file.
     uint64_t file_start_pos_;

     // The file_start_pos_ specifies the starting position, shard_length_ specifies
     // the offset for the chunk, in this file.
     uint64_t shard_length_;

     FileInfo(string f_name_, uint64_t f_start_pos_, uint64_t s_length_) 
     : file_name_(f_name_), file_start_pos_(f_start_pos_), shard_length_(s_length_){}
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     // List of file chunks in a 'shard'.
     vector<FileInfo> file_info_list_;

     // A unique id for the shard.
     uint16_t shard_id_;

     FileShard(vector<FileInfo> f_info_list, uint16_t s_id) : file_info_list_(f_info_list), shard_id_(s_id) {}
};

// Calculates the total size of the input file.
inline uint64_t get_file_size (string file_name) {
     std::streampos begin,end;
     ifstream myfile(file_name);

     begin = myfile.tellg();
     myfile.seekg (0, std::ios::end);
     end = myfile.tellg();
     myfile.close();

     return (end-begin);
}

inline uint64_t get_file_off (ifstream& file_handle, uint64_t curr_offset, uint64_t chunk_size) {
     file_handle.seekg(chunk_size);
     if(file_handle.good()){
          string temp;
          std::getline(file_handle, temp);
     }
     int64_t new_offset = file_handle.tellg();
     return (new_offset != -1 ? new_offset : chunk_size);
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 

inline bool shard_files(const MapReduceSpec& mr_spec, vector<FileShard>& file_shards) {
     vector<string> files = mr_spec.inputs_;
     std::unordered_map<string, uint64_t> file_size_map;

     for(string file : files){
          file_size_map[file] = get_file_size(file);
     }

     uint64_t shard_size = mr_spec.map_kb_*1024;     
     
     vector<FileInfo> curr_file_info_list;
     uint64_t curr_shard_size = shard_size;
     uint16_t shard_id = 0;

     for (string file : files) {
          uint64_t curr_offset = 0;
          uint64_t curr_file_size = file_size_map[file];
          ifstream curr_file_handle(file);

          if (curr_shard_size < curr_file_size) {
               while ((curr_offset + curr_shard_size) < curr_file_size) {
                    uint64_t next_offset = get_file_off(curr_file_handle, curr_offset, curr_offset + curr_shard_size);
                    curr_file_info_list.push_back(*new FileInfo(file, curr_offset, next_offset - curr_offset));
                    file_shards.push_back(*new FileShard(curr_file_info_list, shard_id++));
                    curr_file_info_list.clear();
                    curr_offset = next_offset;
                    curr_shard_size = shard_size;
               }
          }

          uint64_t next_offset = get_file_off(curr_file_handle, curr_offset, curr_file_size);
          curr_file_info_list.push_back(*new FileInfo(file, curr_offset, next_offset - curr_offset));
          curr_shard_size = curr_shard_size - (curr_file_size - curr_offset);
          curr_file_handle.close();
     }

     if (curr_file_info_list.size() > 0) {
          file_shards.push_back(*new FileShard(curr_file_info_list, shard_id++));
          curr_file_info_list.clear(); 
     }

	return true;
}

