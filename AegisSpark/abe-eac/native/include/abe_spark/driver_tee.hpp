#pragma once

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/types.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace abe_spark {

/** Driver TEE 远程认证握手结果（Write Verification 会话） */
struct AttestationResult {
	bool ok;
	std::string session_token;
	std::string reason;
};

/**
 * Spark Driver TEE 动态写能力编排（Task 3）。
 */
class DriverTeeOrchestrator {
public:
	DriverTeeOrchestrator(
		std::shared_ptr<IECDSACrypto> ecdsa,
		const std::string& driver_private_key_pem_path);

	/**
	 * 作业初始化：向 Write Verification 提交 Admin Endorsement 完成一次性远程认证。
	 * write_verification_url 为 TEE 安全通道端点（当前为逻辑占位）。
	 */
	AttestationResult attestationHandshake(
		const std::string& write_verification_url,
		const AdminEndorsement& endorsement);

	/** DAG 分区路径与 Admin 白名单求交，判断目标路径是否可写 */
	static bool isPathAuthorized(
		const std::string& target_path,
		const std::vector<std::string>& allowed_root_directories);

	/** 为单个 Worker Task 派生并签名 Task Ticket */
	TaskTicket deriveTaskTicket(
		const std::string& job_id,
		const std::string& task_id,
		const std::string& worker_ip,
		const std::string& allowed_target_path,
		int64_t expires_at) const;

	bool hasWriteSession() const;
	const std::string& cachedSessionToken() const;

private:
	std::shared_ptr<IECDSACrypto> ecdsa_;
	std::string driver_sk_path_;
	std::string session_token_;
};

} // namespace abe_spark
