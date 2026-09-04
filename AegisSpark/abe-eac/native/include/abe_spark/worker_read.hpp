#pragma once

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/types.hpp"

#include <abe_framework.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace abe_spark {

/** 单列访问计划：DEK 恢复结果 + 掩码策略 */
struct ColumnAccessPlan {
	ColumnByteRange range;
	bool authorized;
	std::vector<uint8_t> dek;
	std::string policy_expression;
	std::string deny_reason;
};

struct WorkerReadResult {
	bool ok;
	std::vector<uint8_t> plaintext;
	std::string reason;
	size_t columns_authorized;
	size_t columns_masked;
	/** 本表内 CP-ABE decryptDek 调用次数与总耗时（毫秒） */
	size_t abe_decrypt_calls;
	double abe_decrypt_ms;
	/** DEK cache 命中次数（跳过 decryptDek） */
	size_t abe_cache_hits;
	/** AES-GCM 列解密总耗时（毫秒） */
	double aes_decrypt_ms;
};

/**
 * Spark Worker TEE 读管道（Task 2）。
 *
 * 物理块布局（DataNode 密文流，按 column_ranges 偏移升序拼接）：
 *   每列: [nonce(12B) | AES-GCM ciphertext(byte_length) | tag(16B)]
 *
 * 输出 plaintext 按 ColumnByteRange 逻辑偏移写入；未授权列填充 ASCII "NULL"。
 */
class WorkerReadPipeline {
public:
	/**
	 * 解析 ABE 安全头，尝试用用户 SK 恢复各列 DEK。
	 * column_ranges 提供列名到物理字节边界的映射。
	 */
	static std::vector<ColumnAccessPlan> buildAccessPlan(
		const Hsec& header,
		const std::vector<ColumnByteRange>& column_ranges,
		ICPABECrypto& cpabe,
		const abe::PublicParams& pp,
		const abe::UserSecretKey& usk,
		size_t* abe_decrypt_calls = nullptr,
		double* abe_decrypt_ms = nullptr,
		std::unordered_map<std::string, std::vector<uint8_t> >* dek_cache = nullptr,
		size_t* abe_cache_hits = nullptr);

	/**
	 * 流式处理 DataNode 密文块：授权列 AES-GCM 解密，未授权列动态 NULL 掩码。
	 */
	static WorkerReadResult processStream(
		const std::vector<uint8_t>& encrypted_stream,
		const std::vector<ColumnAccessPlan>& plan,
		IAESGCMCrypto& aes_gcm,
		int nonce_bytes,
		int tag_bytes);

	/** 将 "NULL" 循环填充至 len 字节（TEE 内存掩码，不改变物理文件布局） */
	static void fillNullMask(uint8_t* dest, size_t len);
};

} // namespace abe_spark
