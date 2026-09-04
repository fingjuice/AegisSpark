#pragma once

#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/types.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace abe_spark {

struct WorkerWriteResult {
	bool ok;
	uint64_t bytes_written;
	std::string reason;
};

/** Worker TEE 直写 HDFS 回调（密文在 Worker 本地，不经 Write Verification 中转） */
class IHdfsDirectWriter {
public:
	virtual ~IHdfsDirectWriter() {}
	virtual WorkerWriteResult write(
		const std::string& target_path,
		const std::vector<uint8_t>& encrypted_data) = 0;
};

/**
 * Spark Worker TEE 写管道。
 * 1. 向 Write Verification 请求写确认（仅提交票据与目标路径，不含密文）
 * 2. 等待并校验 Write Verification 确认指令
 * 3. 在 Worker TEE 内将密文直写 HDFS
 */
class WorkerWritePipeline {
public:
	static WorkerWriteResult execute(
		const TaskTicket& ticket,
		const AdminEndorsement& endorsement,
		const std::string& target_path,
		const std::vector<uint8_t>& encrypted_data,
		int64_t now_epoch_sec,
		WriteVerificationTee& wv,
		IHdfsDirectWriter& hdfs);
};

} // namespace abe_spark
