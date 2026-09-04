#include "abe_spark/spark_config.hpp"

#include "abe_spark/ini_config.hpp"

namespace abe_spark {
namespace {

std::string kv(const std::map<std::string, std::string>& m, const std::string& key, const std::string& def)
{
	std::map<std::string, std::string>::const_iterator it = m.find(key);
	return it == m.end() ? def : it->second;
}

} // namespace

AbeSparkConfig AbeSparkConfigLoader::loadFromFile(const std::string& path)
{
	const std::map<std::string, std::string> ini = IniConfigLoader::loadMapFromFile(path);
	AbeSparkConfig cfg;
	cfg.config_file_path = path;
	cfg.crypto = CryptoConfigLoader::loadFromMap(ini);

	cfg.paths.abe_spark_root = kv(ini, "paths.abe_spark_root", "");
	cfg.paths.abe_eac_root = kv(ini, "paths.abe_eac_root", "");
	cfg.paths.config_dir = kv(ini, "paths.config_dir", "");
	cfg.paths.native_build_dir = kv(ini, "paths.native_build_dir", "");
	cfg.paths.result_dir = kv(ini, "paths.result_dir", "");
	cfg.paths.dataset_dir = kv(ini, "paths.dataset_dir", "");
	cfg.paths.experiment_keys_dir = kv(ini, "paths.experiment_keys_dir", "");
	cfg.paths.hdfs_data_root = kv(ini, "paths.hdfs_data_root", "hdfs://cluster/data/");

	cfg.native.mcl_root = kv(ini, "native.mcl_root", "");
	cfg.native.mcl_build_dir = kv(ini, "native.mcl_build_dir", "");
	cfg.native.mcl_lib_dir = kv(ini, "native.mcl_lib_dir", "");
	cfg.native.jni_library_name = kv(ini, "native.jni_library_name", "abe_spark_jni");
	cfg.native.jni_library_dir = kv(ini, "native.jni_library_dir", "");
	cfg.native.openssl_required = IniConfigLoader::parseBool(kv(ini, "native.openssl_required", "true"), true);

	cfg.spark.enabled = IniConfigLoader::parseBool(kv(ini, "spark.enabled", "false"), false);
	cfg.spark.user_id = kv(ini, "spark.user_id", "");
	cfg.spark.user_attributes = kv(ini, "spark.user_attributes", "");
	cfg.spark.executor_native_library_path = kv(ini, "spark.executor_native_library_path", "");
	cfg.spark.read_pipeline_enabled = IniConfigLoader::parseBool(kv(ini, "spark.read_pipeline_enabled", "true"), true);
	cfg.spark.write_pipeline_enabled = IniConfigLoader::parseBool(kv(ini, "spark.write_pipeline_enabled", "true"), true);
	cfg.spark.task_ticket_ttl_sec = IniConfigLoader::parseInt("spark.task_ticket_ttl_sec",
		kv(ini, "spark.task_ticket_ttl_sec", "2000000000"), 2000000000);

	cfg.experiment.target_gb = IniConfigLoader::parseDouble("experiment.target_gb",
		kv(ini, "experiment.target_gb", "100"), 100.0);
	cfg.experiment.checkpoint_interval_gb = IniConfigLoader::parseDouble("experiment.checkpoint_interval_gb",
		kv(ini, "experiment.checkpoint_interval_gb", "10"), 10.0);
	cfg.experiment.parallel_workers = IniConfigLoader::parseInt("experiment.parallel_workers",
		kv(ini, "experiment.parallel_workers", "8"), 8);
	cfg.experiment.generate_experiment_keys = IniConfigLoader::parseBool(
		kv(ini, "experiment.generate_experiment_keys", "true"), true);
	cfg.experiment.tee_note = kv(ini, "experiment.tee_note", "");

	cfg.dataset.table_prefix = kv(ini, "dataset.table_prefix", "company_");
	cfg.dataset.table_id_width = IniConfigLoader::parseInt("dataset.table_id_width",
		kv(ini, "dataset.table_id_width", "6"), 6);
	cfg.dataset.records_min = IniConfigLoader::parseInt("dataset.records_min",
		kv(ini, "dataset.records_min", "50"), 50);
	cfg.dataset.records_max = IniConfigLoader::parseInt("dataset.records_max",
		kv(ini, "dataset.records_max", "300"), 300);
	cfg.dataset.policy_public = kv(ini, "dataset.policy_public", "role:analyst");
	cfg.dataset.policy_sensitive = kv(ini, "dataset.policy_sensitive", "(role:admin and clearance:5)");
	cfg.dataset.sensitive_columns = IniConfigLoader::splitList(kv(ini, "dataset.sensitive_columns", ""));
	cfg.dataset.columns = IniConfigLoader::splitList(kv(ini, "dataset.columns", ""));
	{
		const std::vector<std::string> widths = IniConfigLoader::splitList(kv(ini, "dataset.column_widths", ""));
		for (size_t i = 0; i < widths.size(); ++i) {
			cfg.dataset.column_widths.push_back(static_cast<size_t>(atoi(widths[i].c_str())));
		}
	}

	return cfg;
}

AbeSparkConfig AbeSparkConfigLoader::loadResolved()
{
	return loadFromFile(IniConfigLoader::resolveConfigPath());
}

} // namespace abe_spark
