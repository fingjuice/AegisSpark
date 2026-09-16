/**
 * 单次写/读请求端到端计时（与 CompanyAbeBenchmark 单表路径一致）：
 * - 写：encryptCompanyTable + EAC gate + 本地文件写入
 * - 读：本地文件读取 + processWorkerRead（CP-ABE 解密 DEK + AES-GCM 列解密）
 */
#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/driver_tee.hpp"
#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/ini_config.hpp"
#include "abe_spark/runtime.hpp"
#include "abe_spark/worker_encrypt.hpp"
#include "abe_spark/worker_write.hpp"

#include <chrono>
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include <openssl/evp.h>
#include <openssl/pem.h>

namespace {

double elapsedMs(const std::chrono::steady_clock::time_point& t0,
	const std::chrono::steady_clock::time_point& t1)
{
	return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

bool writeFile(const std::string& path, const std::vector<uint8_t>& data)
{
	std::ofstream out(path.c_str(), std::ios::binary);
	if (!out) return false;
	if (!data.empty()) {
		out.write(reinterpret_cast<const char*>(&data[0]), static_cast<std::streamsize>(data.size()));
	}
	return out.good();
}

bool readFile(const std::string& path, std::vector<uint8_t>& data)
{
	std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
	if (!in) return false;
	const std::streamsize len = in.tellg();
	if (len < 0) return false;
	in.seekg(0, std::ios::beg);
	data.resize(static_cast<size_t>(len));
	if (len > 0) {
		in.read(reinterpret_cast<char*>(&data[0]), len);
	}
	return in.good();
}

bool generateTestKeyPair(const std::string& privPath, const std::string& pubPath)
{
	EVP_PKEY* pkey = NULL;
	EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL);
	if (!ctx) return false;
	bool ok = false;
	do {
		if (EVP_PKEY_keygen_init(ctx) <= 0) break;
		if (EVP_PKEY_keygen(ctx, &pkey) <= 0) break;
		FILE* fp = fopen(privPath.c_str(), "w");
		if (!fp) break;
		if (PEM_write_PrivateKey(fp, pkey, NULL, NULL, 0, NULL, NULL) != 1) {
			fclose(fp);
			break;
		}
		fclose(fp);
		fp = fopen(pubPath.c_str(), "w");
		if (!fp) break;
		if (PEM_write_PUBKEY(fp, pkey) != 1) {
			fclose(fp);
			break;
		}
		fclose(fp);
		ok = true;
	} while (0);
	EVP_PKEY_CTX_free(ctx);
	EVP_PKEY_free(pkey);
	return ok;
}

} // namespace

int main()
{
	const std::string cfgPath = abe_spark::IniConfigLoader::resolveConfigPath();
	abe_spark::CryptoConfig cfg = abe_spark::CryptoConfigLoader::loadFromFile(cfgPath);

	const std::string adminPriv = "/tmp/abe_single_req_admin.pem";
	const std::string adminPub = "/tmp/abe_single_req_admin.pub";
	const std::string driverPriv = "/tmp/abe_single_req_driver.pem";
	const std::string driverPub = "/tmp/abe_single_req_driver.pub";
	const std::string eacPriv = "/tmp/abe_single_req_eac.pem";
	const std::string eacPub = "/tmp/abe_single_req_eac.pub";
	if (!generateTestKeyPair(adminPriv, adminPub) ||
		!generateTestKeyPair(driverPriv, driverPub) ||
		!generateTestKeyPair(eacPriv, eacPub)) {
		fprintf(stderr, "key generation failed\n");
		return 1;
	}
	cfg.admin_private_key_path = adminPriv;
	cfg.driver_public_key_path = driverPub;

	abe_spark::CryptoProvider provider(cfg);
	abe_spark::CryptoConfig driverCfg = cfg;
	driverCfg.admin_private_key_path = adminPriv;
	abe_spark::CryptoProvider driverProvider(driverCfg);

	const std::string policyPublic = "role:analyst";
	const std::string policySensitive = "(role:admin and clearance:5)";
	const std::string target = "/tmp/abe_single_request_bench.enc";
	const std::string hdfsPrefix = "file:///tmp/abe_single_req/";

	// --- Write request ---
	const auto write0 = std::chrono::steady_clock::now();

	abe_spark::CompanyEncryptResult enc = abe_spark::encryptCompanyTable(
		provider, 1, 0, policyPublic, policySensitive, hdfsPrefix, nullptr);
	if (!enc.ok) {
		fprintf(stderr, "encryptCompanyTable failed: %s\n", enc.reason.c_str());
		return 1;
	}

	abe_spark::AdminEndorsement endorsement = provider.kgc()->endorseSparkJob(
		std::vector<uint8_t>({'b', 'e', 'n', 'c', 'h'}),
		std::vector<std::string>(1, "file:///tmp/abe_single_req"),
		1718745600);

	abe_spark::WriteVerificationTee gatekeeper(
		provider.ecdsa(), adminPub, driverPub, eacPriv, eacPub);

	abe_spark::DriverTeeOrchestrator driver(driverProvider.ecdsa(), driverPriv);
	abe_spark::TaskTicket ticket = driver.deriveTaskTicket(
		"single-req", "task-1", "127.0.0.1", enc.header.file_path, 2000000000);

	abe_spark::WriteVerifyResult verify = gatekeeper.verifyWriteChain(
		ticket, endorsement, enc.header.file_path, 1718746000);
	if (!verify.ok) {
		fprintf(stderr, "Write Verification failed: %s\n", verify.reason.c_str());
		return 1;
	}

	if (!writeFile(target, enc.ciphertext)) {
		fprintf(stderr, "local write failed\n");
		return 1;
	}

	const auto write1 = std::chrono::steady_clock::now();
	const double writeTotalMs = elapsedMs(write0, write1);
	const double writeAbeMs = enc.abe_encrypt_ms;

	// --- Read request ---
	if (!abe_spark::AbeRuntime::instance().init(cfgPath)) {
		fprintf(stderr, "AbeRuntime init failed\n");
		return 1;
	}
	abe::AttributeSet attrs;
	attrs.insert("role:analyst");
	attrs.insert("role:admin");
	attrs.insert("clearance:5");
	attrs.insert("dept:finance");
	abe_spark::AbeRuntime::instance().setUserContext("bench_user", attrs);
	abe_spark::AbeRuntime::instance().setDekCacheEnabled(false);

	const auto read0 = std::chrono::steady_clock::now();

	std::vector<uint8_t> ciphertext;
	if (!readFile(target, ciphertext)) {
		fprintf(stderr, "local read failed\n");
		return 1;
	}

	abe_spark::WorkerReadResult readResult = abe_spark::AbeRuntime::instance().processWorkerRead(
		ciphertext, enc.header, enc.ranges);
	if (!readResult.ok) {
		fprintf(stderr, "processWorkerRead failed: %s\n", readResult.reason.c_str());
		return 1;
	}

	const auto read1 = std::chrono::steady_clock::now();
	const double readTotalMs = elapsedMs(read0, read1);
	const double readAbeMs = readResult.abe_decrypt_ms;

	printf("=== ABE-Spark single request benchmark ===\n");
	printf("config=%s\n", cfgPath.c_str());
	printf("engine=%s\n", cfg.abe_engine.c_str());
	printf("table=%s records=%u plain_bytes=%llu enc_bytes=%llu\n",
		enc.table_name.c_str(), enc.record_count,
		(unsigned long long)enc.plain_bytes, (unsigned long long)enc.enc_bytes);
	printf("\n[WRITE] request_total_ms=%.3f abe_encrypt_ms=%.3f\n", writeTotalMs, writeAbeMs);
	printf("[READ]  request_total_ms=%.3f abe_decrypt_ms=%.3f\n", readTotalMs, readAbeMs);
	printf("abe_decrypt_calls=%zu aes_decrypt_ms=%.3f\n",
		readResult.abe_decrypt_calls, readResult.aes_decrypt_ms);
	return 0;
}
