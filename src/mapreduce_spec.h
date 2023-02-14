#ifndef MAPREDUCE_SPEC_H
#define MAPREDUCE_SPEC_H

#include <string>
#include <bits/stdc++.h>

#define ll long long

using std::ifstream;
using std::string;
using std::vector;
using std::cout;
using std::endl;

// A data structure to store the input configuration for the map-reduce program.
struct MapReduceSpec {
	// Number of workers to spawn.
	int32_t n_workers_;

	// Vector to store the address of each of the workers.
	vector<string> worker_addr_;

	// List of input files to read for performing map reduce. 
	vector<string> inputs_;

	// Directory where the output of the mapreduce operation should be stored.
	string output_dir_;

	// Number of outputs to be generated, this is essentially number of reduce tasks
	// as each reduce task would produce one output. 
	int32_t n_outputs_;

	// Shard size for dividing input across workers.
	ll map_kb_;

	// ID of the user, this is necessary so that the desired map and reduce implementation
	// is invoked.
	string user_id_;

	// Function to print the specs collected from the config file.
	void PrintSpec() {
		cout << "Workers : " << n_workers_ << endl;

		cout << "Output Dir : " << output_dir_ << endl;

		cout << "MapKb : " << map_kb_ << endl;

		cout << "UserId : " << user_id_ << endl;

		for (string& worker: worker_addr_) {
			cout << worker << " ";
		}
		cout << endl;

		for (string& ipfile: inputs_) {
			cout << ipfile << " ";
		}
		cout << endl;
	}
};

// Tokenize input string using the given delimiter.
inline vector<string> SplitFile(string s, string delim) {
	vector<string> tokens;
	string token;

	size_t pos = s.find(delim);
	while (pos != string::npos) {
		token = s.substr(0, pos);
		tokens.push_back(token);
		s.erase(0, pos + delim.length());
		pos = s.find(delim);
	}
	tokens.push_back(s);
	
	return tokens;
}


// Function to read the configuration line by line.
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	ifstream cfg_file(config_filename);

	string cur_line;
	while (getline(cfg_file, cur_line)) {
		vector<string> tokens = SplitFile(cur_line, "=");

		if (tokens[0] == "n_workers") {
			mr_spec.n_workers_ = stoll(tokens[1]);
		} else if (tokens[0] == "worker_ipaddr_ports") {
			mr_spec.worker_addr_ = SplitFile(tokens[1], ",");
		} else if (tokens[0] == "input_files") {
			mr_spec.inputs_ = SplitFile(tokens[1], ",");
		} else if (tokens[0] == "output_dir") {
			mr_spec.output_dir_ = tokens[1];
		} else if (tokens[0] == "n_output_files") {
			mr_spec.n_outputs_ = stoll(tokens[1]);
		} else if (tokens[0] == "map_kilobytes") {
			mr_spec.map_kb_ = stoll(tokens[1]);
		} else if (tokens[0] == "user_id") {
			mr_spec.user_id_ = tokens[1];
		} else {
			return false;
		}
	}

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if (mr_spec.n_workers_ <= 0) {
		return false;
	}

	if (mr_spec.n_workers_ != mr_spec.worker_addr_.size()) {
		return false;
	}

	if (mr_spec.n_workers_ <= 0) {
		return false;
	}

	return true;
}

#endif