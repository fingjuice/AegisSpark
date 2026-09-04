#pragma once

#include "abe_spark/crypto_config.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace abe_spark {

struct PathsConfig {
	std::string abe_spark_root;
	std::string abe_eac_root;
	std::string config_dir;
	std::string native_build_dir;
	std::string result_dir;
	std::string dataset_dir;
	std::string experiment_keys_dir;
	std::string hdfs_data_root;
};

struct NativeConfig {
	std::string mcl_root;
	std::string mcl_build_dir;
	std::string mcl_lib_dir;
	std::string jni_library_name;
	std::string jni_library_dir;
	bool openssl_required;
};

struct SparkRuntimeConfig {
	bool enabled;
	std::string user_id;
	std::string user_attributes;
	std::string executor_native_library_path;
	bool read_pipeline_enabled;
	bool write_pipeline_enabled;
	int64_t task_ticket_ttl_sec;
};

struct ExperimentConfig {
	double target_gb;
	double checkpoint_interval_gb;
	int parallel_workers;
	bool generate_experiment_keys;
	std::string tee_note;
};

struct DatasetConfig {
	std::string table_prefix;
	int table_id_width;
	int records_min;
	int records_max;
	std::string policy_public;
	std::string policy_sensitive;
	std::vector<std::string> sensitive_columns;
	std::vector<std::string> columns;
	std::vector<size_t> column_widths;
};

struct AbeSparkConfig {
	CryptoConfig crypto;
	PathsConfig paths;
	NativeConfig native;
	SparkRuntimeConfig spark;
	ExperimentConfig experiment;
	DatasetConfig dataset;
	std::string config_file_path;
};

class AbeSparkConfigLoader {
public:
	static AbeSparkConfig loadFromFile(const std::string& path);
	static AbeSparkConfig loadResolved();
};

} // namespace abe_spark
