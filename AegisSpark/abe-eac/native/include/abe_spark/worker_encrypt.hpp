#pragma once

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/types.hpp"

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace abe_spark {

struct CompanyEncryptResult {
	bool ok;
	std::string table_name;
	uint32_t record_count;
	uint64_t plain_bytes;
	uint64_t enc_bytes;
	double abe_encrypt_ms;
	/** 本表实际 CP-ABE encryptDek 次数（shared DEK 复用时可为 0） */
	uint32_t abe_encrypt_calls;
	/** AES-GCM 列加密累计耗时（毫秒） */
	double aes_encrypt_ms;
	std::vector<uint8_t> ciphertext;
	Hsec header;
	std::vector<ColumnByteRange> ranges;
	std::string reason;
};

/** 进程级策略 DEK 复用（写侧）：同 policy 共用明文 DEK + encrypted_dek */
struct SharedDekStore {
	std::mutex mu;
	std::map<std::string, std::pair<std::vector<uint8_t>, std::string> > by_policy;
};

/**
 * 生成 company_XXXXXX 列式明文并 CP-ABE + AES-GCM 加密（Spark Worker 写路径）。
 * shared != nullptr 时同 policy 复用 DEK（消融实验 DEK cache 命中前提）。
 */
CompanyEncryptResult encryptCompanyTable(
	CryptoProvider& provider,
	uint64_t company_id,
	uint32_t record_count,
	const std::string& policy_public,
	const std::string& policy_sensitive,
	const std::string& hdfs_file_path_prefix,
	SharedDekStore* shared = nullptr);

} // namespace abe_spark
