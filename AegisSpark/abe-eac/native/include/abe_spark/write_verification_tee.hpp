#pragma once

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/types.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace abe_spark {

struct WriteVerifyResult {
	bool ok;
	std::string reason;
};

/** Write Verification 下发给 Worker TEE 的写确认指令（不含密文） */
struct WriteConfirmInstruction {
	bool ok;
	std::string target_path;
	std::string job_id;
	std::string task_id;
	std::string worker_ip;
	uint64_t payload_bytes;
	int64_t issued_at;
	std::string wv_signature;
	std::string reason;
};

/**
 * Server Write Verification TEE 写门控（Task 4）。
 * 仅验证信任链并向 Worker 下发写确认指令，不接收密文、不代理写 HDFS。
 */
class WriteVerificationTee {
public:
	WriteVerificationTee(
		std::shared_ptr<IECDSACrypto> ecdsa,
		const std::string& admin_public_key_pem_path,
		const std::string& driver_public_key_pem_path,
		const std::string& wv_private_key_pem_path,
		const std::string& wv_public_key_pem_path);

	/** 多层信任链验证（不写入、不下发确认） */
	WriteVerifyResult verifyWriteChain(
		const TaskTicket& ticket,
		const AdminEndorsement& endorsement,
		const std::string& target_path,
		int64_t now_epoch_sec) const;

	/**
	 * 验证通过后签发写确认指令（仅元数据 + WV signature，不含密文）。
	 * Worker 收到确认后在本地 TEE 内直写 HDFS。
	 */
	WriteConfirmInstruction issueWriteConfirm(
		const TaskTicket& ticket,
		const AdminEndorsement& endorsement,
		const std::string& target_path,
		uint64_t payload_bytes,
		int64_t now_epoch_sec);

	/** Worker 侧校验 Write Verification 确认指令签名 */
	bool verifyWriteConfirm(const WriteConfirmInstruction& confirm) const;

private:
	std::shared_ptr<IECDSACrypto> ecdsa_;
	std::string admin_vk_path_;
	std::string driver_vk_path_;
	std::string wv_sk_path_;
	std::string wv_vk_path_;
};

} // namespace abe_spark
