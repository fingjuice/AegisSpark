#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/path_util.hpp"

namespace abe_spark {

WriteVerificationTee::WriteVerificationTee(
	std::shared_ptr<IECDSACrypto> ecdsa,
	const std::string& admin_public_key_pem_path,
	const std::string& driver_public_key_pem_path,
	const std::string& wv_private_key_pem_path,
	const std::string& wv_public_key_pem_path)
	: ecdsa_(ecdsa)
	, admin_vk_path_(admin_public_key_pem_path)
	, driver_vk_path_(driver_public_key_pem_path)
	, wv_sk_path_(wv_private_key_pem_path)
	, wv_vk_path_(wv_public_key_pem_path)
{
}

WriteVerifyResult WriteVerificationTee::verifyWriteChain(
	const TaskTicket& ticket,
	const AdminEndorsement& endorsement,
	const std::string& target_path,
	int64_t now_epoch_sec) const
{
	WriteVerifyResult out;

	std::vector<uint8_t> adminPayload = buildAdminEndorsementPayload(
		endorsement.app_code_hash,
		endorsement.allowed_root_directories,
		endorsement.timestamp);
	VerifyResult adminSig = ecdsa_->verify(
		adminPayload, endorsement.admin_signature, admin_vk_path_);
	if (!adminSig.ok) {
		out.ok = false;
		out.reason = "admin endorsement verification failed: " + adminSig.reason;
		return out;
	}

	std::vector<uint8_t> ticketPayload = buildTaskTicketPayload(
		ticket.job_id, ticket.task_id, ticket.worker_ip,
		ticket.allowed_target_path, ticket.expires_at);
	VerifyResult driverSig = ecdsa_->verify(
		ticketPayload, ticket.driver_signature, driver_vk_path_);
	if (!driverSig.ok) {
		out.ok = false;
		out.reason = "driver task ticket verification failed: " + driverSig.reason;
		return out;
	}

	if (now_epoch_sec > ticket.expires_at) {
		out.ok = false;
		out.reason = "task ticket expired";
		return out;
	}

	if (!PathUtil::isUnderAnyRoot(target_path, endorsement.allowed_root_directories)) {
		out.ok = false;
		out.reason = "target path outside admin whitelist";
		return out;
	}
	if (!PathUtil::ticketBindsTarget(ticket.allowed_target_path, target_path)) {
		out.ok = false;
		out.reason = "target path outside task ticket scope";
		return out;
	}

	out.ok = true;
	return out;
}

WriteConfirmInstruction WriteVerificationTee::issueWriteConfirm(
	const TaskTicket& ticket,
	const AdminEndorsement& endorsement,
	const std::string& target_path,
	uint64_t payload_bytes,
	int64_t now_epoch_sec)
{
	WriteConfirmInstruction out;
	WriteVerifyResult verify = verifyWriteChain(ticket, endorsement, target_path, now_epoch_sec);
	if (!verify.ok) {
		out.ok = false;
		out.reason = verify.reason;
		return out;
	}

	out.target_path = target_path;
	out.job_id = ticket.job_id;
	out.task_id = ticket.task_id;
	out.worker_ip = ticket.worker_ip;
	out.payload_bytes = payload_bytes;
	out.issued_at = now_epoch_sec;

	std::vector<uint8_t> confirmPayload = buildWriteConfirmPayload(
		ticket.job_id, ticket.task_id, ticket.worker_ip,
		target_path, payload_bytes, now_epoch_sec);
	SignResult sig = ecdsa_->sign(confirmPayload, wv_sk_path_);
	if (!sig.ok) {
		out.ok = false;
		out.reason = "Write Verification confirm signature failed: " + sig.reason;
		return out;
	}

	out.wv_signature = sig.signature_hex;
	out.ok = true;
	return out;
}

bool WriteVerificationTee::verifyWriteConfirm(const WriteConfirmInstruction& confirm) const
{
	if (!confirm.ok) return false;
	std::vector<uint8_t> payload = buildWriteConfirmPayload(
		confirm.job_id, confirm.task_id, confirm.worker_ip,
		confirm.target_path, confirm.payload_bytes, confirm.issued_at);
	VerifyResult sig = ecdsa_->verify(payload, confirm.wv_signature, wv_vk_path_);
	return sig.ok;
}

} // namespace abe_spark
