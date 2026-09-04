#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/ini_config.hpp"
#include "abe_spark/types_json.hpp"

#include <abe_framework.hpp>

#include <cstdio>
#include <string>
#include <vector>

static int g_failures = 0;

#define EXPECT_TRUE(cond, msg) \
	do { \
		if (!(cond)) { \
			fprintf(stderr, "FAIL: %s\n", msg); \
			++g_failures; \
		} \
	} while (0)

int main()
{
	// --- JSON round-trip ---
	abe_spark::AdminEndorsement admin;
	admin.app_code_hash = "abc123deadbeef";
	admin.allowed_root_directories.push_back("hdfs://cluster/data/warehouse");
	admin.allowed_root_directories.push_back("hdfs://cluster/tmp/jobs");
	admin.timestamp = 1718745600;
	admin.admin_signature = "aabbcc";

	const std::string adminJson = abe_spark::TypesJson::toJson(admin);
	abe_spark::AdminEndorsement admin2 = abe_spark::TypesJson::adminEndorsementFromJson(adminJson);
	EXPECT_TRUE(admin == admin2, "AdminEndorsement JSON round-trip");

	abe_spark::HsecColumn col;
	col.column_scope.push_back("name");
	col.column_scope.push_back("dept");
	col.policy_expression = "(role:analyst and dept:finance)";
	col.encrypted_dek = "QUJFRGVLCXRlc3Q=";

	abe_spark::Hsec header;
	header.file_path = "hdfs://cluster/data/warehouse/sales/part-00000.parquet";
	header.abe_headers.push_back(col);

	const std::string headerJson = abe_spark::TypesJson::toJson(header);
	abe_spark::Hsec header2 = abe_spark::TypesJson::hsecFromJson(headerJson);
	EXPECT_TRUE(header == header2, "Hsec JSON round-trip");

	abe_spark::TaskTicket ticket;
	ticket.job_id = "job-001";
	ticket.task_id = "task-7";
	ticket.worker_ip = "10.0.0.12";
	ticket.allowed_target_path = "hdfs://cluster/tmp/jobs/job-001/part-00007";
	ticket.expires_at = 1718749200;
	ticket.driver_signature = "ddeeff";

	const std::string ticketJson = abe_spark::TypesJson::toJson(ticket);
	abe_spark::TaskTicket ticket2 = abe_spark::TypesJson::taskTicketFromJson(ticketJson);
	EXPECT_TRUE(ticket == ticket2, "TaskTicket JSON round-trip");

	// --- Config loader ---
	abe_spark::CryptoConfig cfg = abe_spark::CryptoConfigLoader::loadFromFile(
		abe_spark::IniConfigLoader::resolveConfigPath());
	EXPECT_TRUE(cfg.curve_name == "BLS12_381", "curve name from config");
	EXPECT_TRUE(cfg.abe_engine == "fame", "abe engine from config");
	EXPECT_TRUE(cfg.aead_key_bits == 128, "aead key bits from config");

	// --- CP-ABE DEK encrypt/decrypt ---
	abe_spark::CryptoProvider provider(cfg);
	abe::AttributeSet aliceAttrs;
	aliceAttrs.insert("role:analyst");
	aliceAttrs.insert("dept:finance");

	abe::UserSecretKey usk = provider.kgc()->issueUserAttributes("alice", aliceAttrs);
	const std::vector<uint8_t> dek(16, 0x42);
	const std::string policy = "(role:analyst and dept:finance)";

	abe_spark::DekEncryptResult enc = provider.cpabe()->encryptDek(
		provider.kgc()->publicParams(), dek, policy);
	EXPECT_TRUE(enc.ok, "CP-ABE DEK encrypt");

	abe_spark::DekDecryptResult dec = provider.cpabe()->decryptDek(
		provider.kgc()->publicParams(), usk, enc.encrypted_dek_base64, policy);
	EXPECT_TRUE(dec.ok, "CP-ABE DEK decrypt");
	EXPECT_TRUE(dec.dek == dek, "CP-ABE DEK plaintext match");

	abe::AttributeSet bobAttrs;
	bobAttrs.insert("role:guest");
	abe::UserSecretKey bob = provider.kgc()->issueUserAttributes("bob", bobAttrs);
	abe_spark::DekDecryptResult denied = provider.cpabe()->decryptDek(
		provider.kgc()->publicParams(), bob, enc.encrypted_dek_base64, policy);
	EXPECT_TRUE(!denied.ok, "CP-ABE policy denial for unauthorized user");

	// --- SHA256 hex ---
	const std::vector<uint8_t> jarBytes = {'s', 'p', 'a', 'r', 'k', '-', 'j', 'a', 'r'};
	const std::string hash = abe_spark::sha256Hex(jarBytes);
	EXPECT_TRUE(hash.size() == 64, "sha256 hex length");

	if (g_failures == 0) {
		printf("abe_spark_types_test: all checks passed\n");
		return 0;
	}
	fprintf(stderr, "abe_spark_types_test: %d failure(s)\n", g_failures);
	return 1;
}
