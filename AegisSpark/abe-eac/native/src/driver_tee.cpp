#include "abe_spark/driver_tee.hpp"
#include "abe_spark/path_util.hpp"

#include <sstream>
#include <stdexcept>

namespace abe_spark {

DriverTeeOrchestrator::DriverTeeOrchestrator(
	std::shared_ptr<IECDSACrypto> ecdsa,
	const std::string& driver_private_key_pem_path)
	: ecdsa_(ecdsa)
	, driver_sk_path_(driver_private_key_pem_path)
{
}

AttestationResult DriverTeeOrchestrator::attestationHandshake(
	const std::string& write_verification_url,
	const AdminEndorsement& endorsement)
{
	AttestationResult out;
	if (write_verification_url.empty()) {
		out.ok = false;
		out.reason = "empty Write Verification URL";
		return out;
	}
	if (endorsement.app_code_hash.empty() || endorsement.admin_signature.empty()) {
		out.ok = false;
		out.reason = "invalid admin endorsement";
		return out;
	}

	// TEE-to-TEE 远程认证占位：基于 endorsement 派生会话令牌
	std::ostringstream oss;
	oss << "wv-session:" << endorsement.app_code_hash << ":" << endorsement.timestamp;
	session_token_ = oss.str();
	out.ok = true;
	out.session_token = session_token_;
	return out;
}

bool DriverTeeOrchestrator::isPathAuthorized(
	const std::string& target_path,
	const std::vector<std::string>& allowed_root_directories)
{
	return PathUtil::isUnderAnyRoot(target_path, allowed_root_directories);
}

TaskTicket DriverTeeOrchestrator::deriveTaskTicket(
	const std::string& job_id,
	const std::string& task_id,
	const std::string& worker_ip,
	const std::string& allowed_target_path,
	int64_t expires_at) const
{
	TaskTicket ticket;
	ticket.job_id = job_id;
	ticket.task_id = task_id;
	ticket.worker_ip = worker_ip;
	ticket.allowed_target_path = allowed_target_path;
	ticket.expires_at = expires_at;

	std::vector<uint8_t> payload = buildTaskTicketPayload(
		job_id, task_id, worker_ip, allowed_target_path, expires_at);
	SignResult sig = ecdsa_->sign(payload, driver_sk_path_);
	if (!sig.ok) {
		throw std::runtime_error("deriveTaskTicket sign failed: " + sig.reason);
	}
	ticket.driver_signature = sig.signature_hex;
	return ticket;
}

bool DriverTeeOrchestrator::hasWriteSession() const
{
	return !session_token_.empty();
}

const std::string& DriverTeeOrchestrator::cachedSessionToken() const
{
	return session_token_;
}

} // namespace abe_spark
