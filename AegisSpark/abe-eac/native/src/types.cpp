#include "abe_spark/types.hpp"

namespace abe_spark {

bool AdminEndorsement::operator==(const AdminEndorsement& other) const
{
	return app_code_hash == other.app_code_hash &&
		allowed_root_directories == other.allowed_root_directories &&
		timestamp == other.timestamp &&
		admin_signature == other.admin_signature;
}

bool HsecColumn::operator==(const HsecColumn& other) const
{
	return column_scope == other.column_scope &&
		policy_expression == other.policy_expression &&
		encrypted_dek == other.encrypted_dek;
}

bool Hsec::operator==(const Hsec& other) const
{
	return file_path == other.file_path && abe_headers == other.abe_headers;
}

bool TaskTicket::operator==(const TaskTicket& other) const
{
	return job_id == other.job_id &&
		task_id == other.task_id &&
		worker_ip == other.worker_ip &&
		allowed_target_path == other.allowed_target_path &&
		expires_at == other.expires_at &&
		driver_signature == other.driver_signature;
}

bool ColumnByteRange::operator==(const ColumnByteRange& other) const
{
	return column_name == other.column_name &&
		byte_offset == other.byte_offset &&
		byte_length == other.byte_length;
}

} // namespace abe_spark
