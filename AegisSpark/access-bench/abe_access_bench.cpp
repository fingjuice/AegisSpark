/**
 * ABE/Write-Verification TEE Company 数据集 100GB 性能基准测试。
 * 写入：EAC 验证时间、ABE 加密开销、额外存储空间
 * 读取：ABE 查表时间、解密时间
 * 每 10GB 记录 CSV 到 result/company/
 */
#include "hdfs_io.hpp"

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/driver_tee.hpp"
#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/ini_config.hpp"
#include "abe_spark/spark_config.hpp"
#include "abe_spark/types_json.hpp"
#include "abe_spark/worker_encrypt.hpp"
#include "abe_spark/worker_read.hpp"
#include "abe_spark/worker_write.hpp"

#include <abe_framework.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {
const uint64_t kGb = 1024ULL * 1024ULL * 1024ULL;

double nowMs()
{
	using clock = std::chrono::steady_clock;
	return std::chrono::duration<double, std::milli>(clock::now().time_since_epoch()).count();
}

std::string isoTimestamp()
{
	std::time_t t = std::time(NULL);
	char buf[32];
	std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&t));
	return std::string(buf);
}

std::string envOr(const char* name, const std::string& def)
{
	const char* v = std::getenv(name);
	return v != NULL ? std::string(v) : def;
}

uint64_t computeAbeHeaderBytes(
	const abe_spark::Hsec& header,
	const std::vector<abe_spark::ColumnByteRange>& ranges)
{
	return abe_spark::TypesJson::toJson(header).size()
		+ abe_spark::TypesJson::toJsonArray(ranges).size();
}

struct BenchStats {
	uint64_t cum_tables;
	uint64_t cum_rows;
	uint64_t cum_plain_bytes;
	uint64_t cum_enc_bytes;
	uint64_t cum_abe_header_bytes;
	double encrypt_ms;
	double eac_verify_ms;
	double hdfs_write_ms;
	double abe_lookup_ms;
	double decrypt_ms;
	double hdfs_read_ms;
};

void resetStats(BenchStats& s)
{
	s.cum_tables = 0;
	s.cum_rows = 0;
	s.cum_plain_bytes = 0;
	s.cum_enc_bytes = 0;
	s.cum_abe_header_bytes = 0;
	s.encrypt_ms = 0;
	s.eac_verify_ms = 0;
	s.hdfs_write_ms = 0;
	s.abe_lookup_ms = 0;
	s.decrypt_ms = 0;
	s.hdfs_read_ms = 0;
}

BenchStats zeroStats()
{
	BenchStats s;
	resetStats(s);
	return s;
}

void appendWriteCsv(
	const std::string& path,
	bool append,
	int checkpoint_gb,
	const BenchStats& s,
	double total_ms)
{
	std::ofstream out(path.c_str(), append ? std::ios::app : std::ios::trunc);
	if (!append) {
		out << "checkpoint_gb,cumulative_tables,cumulative_rows,cumulative_bytes,"
			<< "abe_header_bytes,abe_overhead_bytes,eac_verify_time_total_ms,"
			<< "encrypt_time_total_ms,hdfs_write_time_total_ms,total_elapsed_ms,timestamp\n";
	}
	const uint64_t overhead = (s.cum_enc_bytes > s.cum_plain_bytes
		? s.cum_enc_bytes - s.cum_plain_bytes : 0) + s.cum_abe_header_bytes;
	out << checkpoint_gb << ","
		<< s.cum_tables << ","
		<< s.cum_rows << ","
		<< s.cum_plain_bytes << ","
		<< s.cum_abe_header_bytes << ","
		<< overhead << ","
		<< std::fixed << std::setprecision(3)
		<< s.eac_verify_ms << ","
		<< s.encrypt_ms << ","
		<< s.hdfs_write_ms << ","
		<< total_ms << ","
		<< isoTimestamp() << "\n";
}

void appendReadCsv(
	const std::string& path,
	bool append,
	int checkpoint_gb,
	const BenchStats& s,
	double total_ms)
{
	std::ofstream out(path.c_str(), append ? std::ios::app : std::ios::trunc);
	if (!append) {
		out << "checkpoint_gb,cumulative_tables,cumulative_rows,cumulative_bytes_read,"
			<< "abe_lookup_time_total_ms,decrypt_time_total_ms,hdfs_read_time_total_ms,"
			<< "total_elapsed_ms,timestamp\n";
	}
	out << checkpoint_gb << ","
		<< s.cum_tables << ","
		<< s.cum_rows << ","
		<< s.cum_plain_bytes << ","
		<< std::fixed << std::setprecision(3)
		<< s.abe_lookup_ms << ","
		<< s.decrypt_ms << ","
		<< s.hdfs_read_ms << ","
		<< total_ms << ","
		<< isoTimestamp() << "\n";
}

struct WorkerContext {
	abe_spark::CryptoProvider* provider;
	std::mutex* crypto_mu;
	abe_spark::WriteVerificationTee* wv;
	abe_spark::DriverTeeOrchestrator* driver;
	abe_spark::AdminEndorsement endorsement;
	abe_bench::HdfsClient* hdfs;
	std::string hdfs_prefix;
	std::string job_id;
	std::string worker_ip;
	int64_t now_epoch;
	const abe_spark::AbeSparkConfig* cfg;
};

bool writeOneTable(
	WorkerContext& ctx,
	uint64_t company_id,
	BenchStats& local)
{
	const abe_spark::AbeSparkConfig& cfg = *ctx.cfg;
	const double t_enc0 = nowMs();
	abe_spark::CompanyEncryptResult enc;
	{
		std::lock_guard<std::mutex> lock(*ctx.crypto_mu);
		enc = abe_spark::encryptCompanyTable(
			*ctx.provider,
			company_id,
			0,
			cfg.dataset.policy_public,
			cfg.dataset.policy_sensitive,
			ctx.hdfs_prefix);
	}
	const double t_enc1 = nowMs();
	if (!enc.ok) {
		std::cerr << "encrypt failed table " << company_id << ": " << enc.reason << "\n";
		return false;
	}

	const std::string target = enc.header.file_path;
	abe_spark::TaskTicket ticket = ctx.driver->deriveTaskTicket(
		ctx.job_id,
		"task-" + std::to_string(company_id),
		ctx.worker_ip,
		target,
		ctx.now_epoch + cfg.spark.task_ticket_ttl_sec);

	const double t_eac0 = nowMs();
	abe_spark::WriteVerifyResult verify = ctx.wv->verifyWriteChain(
		ticket, ctx.endorsement, target, ctx.now_epoch);
	if (!verify.ok) {
		std::cerr << "Write Verification failed: " << verify.reason << "\n";
		return false;
	}
	abe_spark::WriteConfirmInstruction confirm = ctx.wv->issueWriteConfirm(
		ticket, ctx.endorsement, target, enc.enc_bytes, ctx.now_epoch);
	if (!confirm.ok || !ctx.wv->verifyWriteConfirm(confirm)) {
		std::cerr << "Write Verification confirm failed\n";
		return false;
	}
	const double t_eac1 = nowMs();

	abe_bench::HdfsDirectWriter writer(*ctx.hdfs);
	const double t_hdfs0 = nowMs();
	abe_spark::WorkerWriteResult wr = abe_spark::WorkerWritePipeline::execute(
		ticket, ctx.endorsement, target, enc.ciphertext,
		ctx.now_epoch, *ctx.wv, writer);
	if (!wr.ok) {
		std::cerr << "hdfs write failed: " << wr.reason << "\n";
		return false;
	}
	if (!ctx.hdfs->setTableMeta(target, enc.header, enc.ranges)) {
		std::cerr << "xattr write failed for " << target << "\n";
		return false;
	}
	const double t_hdfs1 = nowMs();

	const uint64_t header_bytes = computeAbeHeaderBytes(enc.header, enc.ranges);
	local.cum_tables += 1;
	local.cum_rows += enc.record_count;
	local.cum_plain_bytes += enc.plain_bytes;
	local.cum_enc_bytes += enc.enc_bytes;
	local.cum_abe_header_bytes += header_bytes;
	local.encrypt_ms += (t_enc1 - t_enc0);
	local.eac_verify_ms += (t_eac1 - t_eac0);
	local.hdfs_write_ms += (t_hdfs1 - t_hdfs0);
	return true;
}

bool readOneTable(
	WorkerContext& ctx,
	const std::string& path,
	abe_spark::ICPABECrypto& cpabe,
	const abe::PublicParams& pp,
	const abe::UserSecretKey& usk,
	BenchStats& local)
{
	abe_bench::HdfsTableMeta meta;
	const double t_hdfs0 = nowMs();
	if (!ctx.hdfs->readTableMeta(path, meta)) {
		return false;
	}
	abe_bench::HdfsReadResult file = ctx.hdfs->readFile(path);
	const double t_hdfs1 = nowMs();
	if (!file.ok) return false;

	const double t_abe0 = nowMs();
	std::vector<abe_spark::ColumnAccessPlan> plan =
		abe_spark::WorkerReadPipeline::buildAccessPlan(
			meta.header, meta.ranges, cpabe, pp, usk);
	const double t_abe1 = nowMs();

	const abe_spark::CryptoConfig& crypto = ctx.provider->config();
	const double t_dec0 = nowMs();
	abe_spark::WorkerReadResult read = abe_spark::WorkerReadPipeline::processStream(
		file.data, plan, *ctx.provider->aesGcm(),
		crypto.aead_nonce_bytes, crypto.aead_tag_bytes);
	const double t_dec1 = nowMs();
	if (!read.ok) return false;

	local.cum_tables += 1;
	if (!meta.ranges.empty()) {
		local.cum_rows += meta.ranges[0].byte_length / 16;
	}
	local.cum_plain_bytes += meta.plain_bytes;
	local.hdfs_read_ms += (t_hdfs1 - t_hdfs0);
	local.abe_lookup_ms += (t_abe1 - t_abe0);
	local.decrypt_ms += (t_dec1 - t_dec0);
	return true;
}

void runWriteBenchmark(
	const abe_spark::AbeSparkConfig& cfg,
	abe_spark::CryptoProvider& provider,
	abe_spark::WriteVerificationTee& wv,
	abe_spark::DriverTeeOrchestrator& driver,
	abe_bench::HdfsClient& hdfs,
	const std::string& result_dir,
	const std::string& mode)
{
	const uint64_t target_bytes = static_cast<uint64_t>(cfg.experiment.target_gb * kGb);
	const int checkpoint_gb = std::max(1, static_cast<int>(cfg.experiment.checkpoint_interval_gb));
	const int workers = std::max(1, cfg.experiment.parallel_workers);
	const std::string write_csv = result_dir + "/write_perf.csv";

	std::string hdfs_prefix = cfg.paths.hdfs_data_root;
	if (!hdfs_prefix.empty() && hdfs_prefix[hdfs_prefix.size() - 1] != '/') {
		hdfs_prefix += "/";
	}
	hdfs.mkdirs(cfg.paths.hdfs_data_root);

	abe_spark::AdminEndorsement endorsement = provider.kgc()->endorseSparkJob(
		std::vector<uint8_t>({'a', 'b', 'e', '-', 'b', 'e', 'n', 'c', 'h'}),
		std::vector<std::string>(1, cfg.paths.hdfs_data_root),
		static_cast<int64_t>(std::time(NULL)));

	driver.attestationHandshake("tee://write-verification:9000", endorsement);

	std::mutex crypto_mu;
	WorkerContext ctx;
	ctx.provider = &provider;
	ctx.crypto_mu = &crypto_mu;
	ctx.wv = &wv;
	ctx.driver = &driver;
	ctx.endorsement = endorsement;
	ctx.hdfs = &hdfs;
	ctx.hdfs_prefix = hdfs_prefix;
	ctx.job_id = "abe-company-bench";
	ctx.worker_ip = "10.26.40.83";
	ctx.now_epoch = static_cast<int64_t>(std::time(NULL));
	ctx.cfg = &cfg;

	std::cout << "[WRITE] target=" << cfg.experiment.target_gb << "GB"
		<< " checkpoint=" << checkpoint_gb << "GB workers=" << workers << "\n";
	std::cout.flush();

	BenchStats global = zeroStats();
	std::mutex global_mu;
	const double wall0 = nowMs();
	std::atomic<uint64_t> next_table(0);
	std::atomic<bool> stop(false);
	std::mutex ck_mu;
	int next_checkpoint = checkpoint_gb;
	bool csv_header_written = false;

	auto worker_fn = [&]() {
		abe_bench::HdfsClient local_hdfs(cfg.paths.hdfs_data_root, envOr("HDFS_USER", "shanlicheng"));
		if (!local_hdfs.connect()) {
			std::cerr << "worker hdfs connect failed\n";
			return;
		}
		WorkerContext local_ctx = ctx;
		local_ctx.hdfs = &local_hdfs;
		BenchStats local = zeroStats();
		for (;;) {
			if (stop.load()) break;
			const uint64_t tid = next_table.fetch_add(1);
			{
				std::lock_guard<std::mutex> lock(global_mu);
				if (global.cum_plain_bytes >= target_bytes) {
					stop.store(true);
					break;
				}
			}
			if (!writeOneTable(local_ctx, tid, local)) continue;
			bool checkpoint = false;
			int ck_gb = 0;
			{
				std::lock_guard<std::mutex> lock(global_mu);
				global.cum_tables += local.cum_tables;
				global.cum_rows += local.cum_rows;
				global.cum_plain_bytes += local.cum_plain_bytes;
				global.cum_enc_bytes += local.cum_enc_bytes;
				global.cum_abe_header_bytes += local.cum_abe_header_bytes;
				global.encrypt_ms += local.encrypt_ms;
				global.eac_verify_ms += local.eac_verify_ms;
				global.hdfs_write_ms += local.hdfs_write_ms;
				resetStats(local);

				const double cum_gb = global.cum_plain_bytes / static_cast<double>(kGb);
				if (cum_gb >= next_checkpoint || global.cum_plain_bytes >= target_bytes) {
					checkpoint = true;
					ck_gb = next_checkpoint;
					next_checkpoint += checkpoint_gb;
				}
			}
			if (checkpoint) {
				std::lock_guard<std::mutex> lock(ck_mu);
				appendWriteCsv(write_csv, csv_header_written, ck_gb, global, nowMs() - wall0);
				csv_header_written = true;
				std::cout << "[WRITE] checkpoint " << ck_gb << "GB tables="
					<< global.cum_tables << " plain="
					<< (global.cum_plain_bytes / kGb) << "GB\n";
				std::cout.flush();
			}
			if (global.cum_plain_bytes >= target_bytes) {
				stop.store(true);
				break;
			}
		}
	};

	std::vector<std::thread> threads;
	for (int i = 0; i < workers; ++i) {
		threads.push_back(std::thread(worker_fn));
	}
	for (size_t i = 0; i < threads.size(); ++i) {
		threads[i].join();
	}

	std::cout << "[WRITE] done tables=" << global.cum_tables
		<< " bytes=" << global.cum_plain_bytes << "\n";
	std::cout.flush();
}

void runReadBenchmark(
	const abe_spark::AbeSparkConfig& cfg,
	abe_spark::CryptoProvider& provider,
	abe_bench::HdfsClient& hdfs,
	const std::string& result_dir)
{
	const uint64_t target_bytes = static_cast<uint64_t>(cfg.experiment.target_gb * kGb);
	const int checkpoint_gb = std::max(1, static_cast<int>(cfg.experiment.checkpoint_interval_gb));
	const std::string read_csv = result_dir + "/read_perf.csv";

	abe::AttributeSet attrs;
	attrs.insert("role:analyst");
	attrs.insert("dept:finance");
	abe::UserSecretKey usk = provider.kgc()->issueUserAttributes("analyst", attrs);

	WorkerContext ctx;
	ctx.provider = &provider;
	ctx.hdfs = &hdfs;
	ctx.cfg = &cfg;

	std::vector<std::string> files = hdfs.listEncFiles(cfg.paths.hdfs_data_root);
	std::cout << "[READ] files=" << files.size() << " target=" << cfg.experiment.target_gb << "GB\n";

	BenchStats global = zeroStats();
	const double wall0 = nowMs();
	int next_checkpoint = checkpoint_gb;
	bool csv_header_written = false;

	for (size_t i = 0; i < files.size(); ++i) {
		if (global.cum_plain_bytes >= target_bytes) break;
		BenchStats local = zeroStats();
		if (!readOneTable(ctx, files[i], *provider.cpabe(),
				provider.kgc()->publicParams(), usk, local)) {
			continue;
		}
		global.cum_tables += local.cum_tables;
		global.cum_rows += local.cum_rows;
		global.cum_plain_bytes += local.cum_plain_bytes;
		global.hdfs_read_ms += local.hdfs_read_ms;
		global.abe_lookup_ms += local.abe_lookup_ms;
		global.decrypt_ms += local.decrypt_ms;

		const double cum_gb = global.cum_plain_bytes / static_cast<double>(kGb);
		if (cum_gb >= next_checkpoint || i + 1 == files.size()) {
			appendReadCsv(read_csv, csv_header_written, next_checkpoint, global, nowMs() - wall0);
			csv_header_written = true;
			std::cout << "[READ] checkpoint " << next_checkpoint << "GB\n";
			next_checkpoint += checkpoint_gb;
		}
	}

	std::cout << "[READ] done tables=" << global.cum_tables << "\n";
}

} // namespace

int main(int argc, char** argv)
{
	(void)argc;
	(void)argv;

	abe_spark::IniConfigLoader::ensureDefaultEnvVars();
	const std::string config_path = abe_spark::IniConfigLoader::resolveConfigPath();
	const abe_spark::AbeSparkConfig cfg_loaded = abe_spark::AbeSparkConfigLoader::loadFromFile(config_path);
	abe_spark::AbeSparkConfig cfg = cfg_loaded;
	if (const char* tg = std::getenv("TARGET_GB")) {
		cfg.experiment.target_gb = atof(tg);
	}
	if (const char* ck = std::getenv("CHECKPOINT_GB")) {
		cfg.experiment.checkpoint_interval_gb = atof(ck);
	}
	if (const char* pw = std::getenv("PARALLEL_WORKERS")) {
		cfg.experiment.parallel_workers = atoi(pw);
	}

	const std::string result_dir = envOr("RESULT_DIR",
		cfg.paths.result_dir.empty()
			? (cfg.paths.abe_spark_root + "/result/company")
			: cfg.paths.result_dir);
	const std::string mode = envOr("BENCH_MODE", "all");
	const std::string hdfs_user = envOr("HDFS_USER", "shanlicheng");

	std::string mkdir_cmd = "mkdir -p " + result_dir;
	std::system(mkdir_cmd.c_str());

	std::cout << "=== ABE/Write-Verification Company Benchmark ===\n";
	std::cout << "Config: " << config_path << "\n";
	std::cout << "Result: " << result_dir << "\n";
	std::cout << "Mode: " << mode << "\n";
	std::cout << "Target: " << cfg.experiment.target_gb << "GB\n";

	abe_spark::CryptoProvider provider(cfg.crypto);
	abe_spark::CryptoProvider driverProvider(cfg.crypto);
	abe_spark::WriteVerificationTee wv(
		provider.ecdsa(),
		cfg.crypto.admin_public_key_path,
		cfg.crypto.driver_public_key_path,
		cfg.crypto.write_verification_private_key_path,
		cfg.crypto.write_verification_public_key_path);
	abe_spark::DriverTeeOrchestrator driver(
		driverProvider.ecdsa(), cfg.crypto.driver_private_key_path);

	abe_bench::HdfsClient hdfs(cfg.paths.hdfs_data_root, hdfs_user);
	if (!hdfs.connect()) {
		std::cerr << "HDFS connect failed to " << cfg.paths.hdfs_data_root << "\n";
		return 1;
	}

	if (mode == "write" || mode == "all") {
		runWriteBenchmark(cfg, provider, wv, driver, hdfs, result_dir, mode);
	}
	if (mode == "read" || mode == "all") {
		runReadBenchmark(cfg, provider, hdfs, result_dir);
	}

	return 0;
}
