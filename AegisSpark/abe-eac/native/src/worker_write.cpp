#include "abe_spark/worker_write.hpp"

namespace abe_spark {

WorkerWriteResult WorkerWritePipeline::execute(
	const TaskTicket& ticket,
	const AdminEndorsement& endorsement,
	const std::string& target_path,
	const std::vector<uint8_t>& encrypted_data,
	int64_t now_epoch_sec,
	WriteVerificationTee& wv,
	IHdfsDirectWriter& hdfs)
{
	WorkerWriteResult out;

	WriteConfirmInstruction confirm = wv.issueWriteConfirm(
		ticket, endorsement, target_path,
		static_cast<uint64_t>(encrypted_data.size()), now_epoch_sec);
	if (!confirm.ok) {
		out.ok = false;
		out.reason = "Write Verification confirm denied: " + confirm.reason;
		return out;
	}

	if (confirm.target_path != target_path
		|| confirm.job_id != ticket.job_id
		|| confirm.task_id != ticket.task_id
		|| confirm.worker_ip != ticket.worker_ip) {
		out.ok = false;
		out.reason = "Write Verification confirm binding mismatch";
		return out;
	}

	if (confirm.payload_bytes != encrypted_data.size()) {
		out.ok = false;
		out.reason = "Write Verification confirm payload size mismatch";
		return out;
	}

	if (!wv.verifyWriteConfirm(confirm)) {
		out.ok = false;
		out.reason = "Write Verification confirm signature verification failed";
		return out;
	}

	return hdfs.write(target_path, encrypted_data);
}

} // namespace abe_spark
