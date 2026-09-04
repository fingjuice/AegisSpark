#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/driver_tee.hpp"
#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/ini_config.hpp"
#include "abe_spark/worker_read.hpp"
#include "abe_spark/worker_write.hpp"

#include <abe_framework.hpp>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <openssl/evp.h>
#include <openssl/pem.h>

static int g_failures = 0;

#define EXPECT_TRUE(cond, msg) \
	do { \
		if (!(cond)) { \
			fprintf(stderr, "FAIL: %s\n", msg); \
			++g_failures; \
		} \
	} while (0)

static bool writePem(const std::string& path, EVP_PKEY* pkey, bool privateKey)
{
	FILE* fp = fopen(path.c_str(), "w");
	if (!fp) return false;
	int ok = privateKey
		? PEM_write_PrivateKey(fp, pkey, NULL, NULL, 0, NULL, NULL)
		: PEM_write_PUBKEY(fp, pkey);
	fclose(fp);
	return ok == 1;
}

static bool generateTestKeyPair(const std::string& privPath, const std::string& pubPath)
{
	EVP_PKEY* pkey = NULL;
	EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, NULL);
	if (!ctx) return false;
	bool ok = false;
	do {
		if (EVP_PKEY_keygen_init(ctx) <= 0) break;
		if (EVP_PKEY_CTX_set_ec_paramgen_curve_nid(ctx, NID_X9_62_prime256v1) <= 0) break;
		if (EVP_PKEY_keygen(ctx, &pkey) <= 0) break;
		if (!writePem(privPath, pkey, true)) break;
		if (!writePem(pubPath, pkey, false)) break;
		ok = true;
	} while (0);
	EVP_PKEY_CTX_free(ctx);
	EVP_PKEY_free(pkey);
	return ok;
}

static std::vector<uint8_t> encryptColumnSegment(
	abe_spark::IAESGCMCrypto& aes,
	const std::vector<uint8_t>& dek,
	const std::vector<uint8_t>& plaintext,
	int nonce_bytes,
	int tag_bytes)
{
	std::vector<uint8_t> nonce(static_cast<size_t>(nonce_bytes), 0x01);
	abe_spark::AesGcmResult enc = aes.encrypt(dek, nonce, plaintext);
	EXPECT_TRUE(enc.ok, "column segment encrypt");
	std::vector<uint8_t> out;
	out.insert(out.end(), nonce.begin(), nonce.end());
	out.insert(out.end(), enc.output.begin(), enc.output.end());
	EXPECT_TRUE(enc.output.size() == plaintext.size() + static_cast<size_t>(tag_bytes), "segment size");
	return out;
}

int main()
{
	const std::string adminPriv = "/tmp/abe_spark_test_admin.pem";
	const std::string adminPub = "/tmp/abe_spark_test_admin.pub";
	const std::string driverPriv = "/tmp/abe_spark_test_driver.pem";
	const std::string driverPub = "/tmp/abe_spark_test_driver.pub";
	const std::string eacPriv = "/tmp/abe_spark_test_eac.pem";
	const std::string eacPub = "/tmp/abe_spark_test_eac.pub";
	EXPECT_TRUE(generateTestKeyPair(adminPriv, adminPub), "generate admin keypair");
	EXPECT_TRUE(generateTestKeyPair(driverPriv, driverPub), "generate driver keypair");
	EXPECT_TRUE(generateTestKeyPair(eacPriv, eacPub), "generate eac keypair");

	abe_spark::CryptoConfig cfg = abe_spark::CryptoConfigLoader::loadFromFile(
		abe_spark::IniConfigLoader::resolveConfigPath());
	cfg.admin_private_key_path = adminPriv;
	cfg.driver_public_key_path = driverPub;

	abe_spark::CryptoProvider provider(cfg);
	abe_spark::CryptoConfig driverCfg = cfg;
	driverCfg.admin_private_key_path = adminPriv;
	abe_spark::CryptoProvider driverProvider(driverCfg);

	// --- Worker read pipeline ---
	abe::AttributeSet analystAttrs;
	analystAttrs.insert("role:analyst");
	analystAttrs.insert("dept:finance");
	abe::UserSecretKey analyst = provider.kgc()->issueUserAttributes("analyst", analystAttrs);

	const std::vector<uint8_t> dekName(16, 0x11);
	const std::vector<uint8_t> dekSalary(16, 0x22);
	const std::string policyBase = "role:analyst";
	const std::string policySensitive = "(role:admin and clearance:5)";

	abe_spark::DekEncryptResult encName = provider.cpabe()->encryptDek(
		provider.kgc()->publicParams(), dekName, policyBase);
	abe_spark::DekEncryptResult encSalary = provider.cpabe()->encryptDek(
		provider.kgc()->publicParams(), dekSalary, policySensitive);
	EXPECT_TRUE(encName.ok && encSalary.ok, "encrypt column DEKs");

	abe_spark::Hsec header;
	header.file_path = "hdfs://cluster/data/employees/part-00000.bin";
	abe_spark::HsecColumn hName;
	hName.column_scope.push_back("name");
	hName.policy_expression = policyBase;
	hName.encrypted_dek = encName.encrypted_dek_base64;
	abe_spark::HsecColumn hSalary;
	hSalary.column_scope.push_back("salary");
	hSalary.policy_expression = policySensitive;
	hSalary.encrypted_dek = encSalary.encrypted_dek_base64;
	header.abe_headers.push_back(hName);
	header.abe_headers.push_back(hSalary);

	std::vector<abe_spark::ColumnByteRange> ranges;
	abe_spark::ColumnByteRange rName;
	rName.column_name = "name";
	rName.byte_offset = 0;
	rName.byte_length = 8;
	abe_spark::ColumnByteRange rSalary;
	rSalary.column_name = "salary";
	rSalary.byte_offset = 8;
	rSalary.byte_length = 8;
	ranges.push_back(rName);
	ranges.push_back(rSalary);

	std::vector<abe_spark::ColumnAccessPlan> plan = abe_spark::WorkerReadPipeline::buildAccessPlan(
		header, ranges, *provider.cpabe(),
		provider.kgc()->publicParams(), analyst);
	EXPECT_TRUE(plan.size() == 2, "access plan size");

	std::vector<uint8_t> plainName(8, 'A');
	std::vector<uint8_t> plainSalary(8, 'S');
	std::vector<uint8_t> encrypted;
	{
		std::vector<uint8_t> seg0 = encryptColumnSegment(
			*provider.aesGcm(), dekName, plainName, cfg.aead_nonce_bytes, cfg.aead_tag_bytes);
		std::vector<uint8_t> seg1 = encryptColumnSegment(
			*provider.aesGcm(), dekSalary, plainSalary, cfg.aead_nonce_bytes, cfg.aead_tag_bytes);
		encrypted.insert(encrypted.end(), seg0.begin(), seg0.end());
		encrypted.insert(encrypted.end(), seg1.begin(), seg1.end());
	}

	abe_spark::WorkerReadResult readResult = abe_spark::WorkerReadPipeline::processStream(
		encrypted, plan, *provider.aesGcm(), cfg.aead_nonce_bytes, cfg.aead_tag_bytes);
	EXPECT_TRUE(readResult.ok, "worker read pipeline");
	EXPECT_TRUE(readResult.columns_authorized == 1, "one authorized column");
	EXPECT_TRUE(readResult.columns_masked == 1, "one masked column");
	EXPECT_TRUE(readResult.plaintext.size() == 16, "plaintext size");
	EXPECT_TRUE(std::memcmp(readResult.plaintext.data(), plainName.data(), 8) == 0, "name decrypted");
	EXPECT_TRUE(std::memcmp(readResult.plaintext.data() + 8, "NULL", 4) == 0, "salary masked NULL");

	// --- Driver TEE + Write Verification trust chain ---
	abe_spark::DriverTeeOrchestrator driver(driverProvider.ecdsa(), driverPriv);
	abe_spark::AttestationResult att = driver.attestationHandshake(
		"tee://write-verification:9000",
		provider.kgc()->endorseSparkJob(
			std::vector<uint8_t>({'s', 'p', 'a', 'r', 'k', '-', 'j', 'a', 'r'}),
			std::vector<std::string>(1, "hdfs://cluster/tmp/jobs"),
			1718745600));
	EXPECT_TRUE(att.ok && driver.hasWriteSession(), "driver attestation");

	const std::string target = "hdfs://cluster/tmp/jobs/job-001/part-00007";
	EXPECT_TRUE(abe_spark::DriverTeeOrchestrator::isPathAuthorized(
		target, std::vector<std::string>(1, "hdfs://cluster/tmp/jobs")), "path authorized");

	abe_spark::TaskTicket ticket = driver.deriveTaskTicket(
		"job-001", "task-7", "10.0.0.12", target, 2000000000);

	abe_spark::AdminEndorsement endorsement = provider.kgc()->endorseSparkJob(
		std::vector<uint8_t>({'s', 'p', 'a', 'r', 'k', '-', 'j', 'a', 'r'}),
		std::vector<std::string>(1, "hdfs://cluster/tmp/jobs"),
		1718745600);

	abe_spark::WriteVerificationTee gatekeeper(
		provider.ecdsa(), adminPub, driverPub, eacPriv, eacPub);

	abe_spark::WriteVerifyResult verify = gatekeeper.verifyWriteChain(
		ticket, endorsement, target, 1718746000);
	EXPECT_TRUE(verify.ok, verify.ok ? "EAC trust chain verify" : verify.reason.c_str());

	abe_spark::WriteConfirmInstruction confirm = gatekeeper.issueWriteConfirm(
		ticket, endorsement, target, 64, 1718746000);
	EXPECT_TRUE(confirm.ok, confirm.ok ? "EAC write confirm" : confirm.reason.c_str());
	EXPECT_TRUE(confirm.target_path == target, "confirm binds target");
	EXPECT_TRUE(confirm.payload_bytes == 64, "confirm binds payload size");
	EXPECT_TRUE(!confirm.wv_signature.empty(), "EAC signature present");

	struct MockHdfsWriter : abe_spark::IHdfsDirectWriter {
		abe_spark::WorkerWriteResult write(
			const std::string& path,
			const std::vector<uint8_t>& data)
		{
			abe_spark::WorkerWriteResult r;
			r.ok = true;
			r.bytes_written = data.size();
			last_path = path;
			return r;
		}
		std::string last_path;
	} hdfs;

	std::vector<uint8_t> payload(64, 0xAB);
	abe_spark::WorkerWriteResult writeResult = abe_spark::WorkerWritePipeline::execute(
		ticket, endorsement, target, payload, 1718746000, gatekeeper, hdfs);
	EXPECT_TRUE(writeResult.ok, "worker TEE direct HDFS write");
	EXPECT_TRUE(writeResult.bytes_written == 64, "bytes written");
	EXPECT_TRUE(hdfs.last_path == target, "hdfs target path");

	// Expired ticket should fail
	abe_spark::TaskTicket expired = driver.deriveTaskTicket(
		"job-001", "task-8", "10.0.0.13", target, 1000);
	abe_spark::WriteVerifyResult expiredVerify = gatekeeper.verifyWriteChain(
		expired, endorsement, target, 1718746000);
	EXPECT_TRUE(!expiredVerify.ok, "expired ticket rejected");

	if (g_failures == 0) {
		printf("abe_spark_pipeline_test: all checks passed\n");
		return 0;
	}
	fprintf(stderr, "abe_spark_pipeline_test: %d failure(s)\n", g_failures);
	return 1;
}
