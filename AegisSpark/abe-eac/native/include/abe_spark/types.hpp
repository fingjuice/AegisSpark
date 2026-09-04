#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace abe_spark {

/** 系统管理员签发的 Spark 作业初始写授权（离线信任锚） */
struct AdminEndorsement {
	std::string app_code_hash;
	std::vector<std::string> allowed_root_directories;
	int64_t timestamp;
	std::string admin_signature;

	bool operator==(const AdminEndorsement& other) const;
};

/** 单列/列组 CP-ABE 安全头项 */
struct HsecColumn {
	std::vector<std::string> column_scope;
	std::string policy_expression;
	/** Base64 编码的 CP-ABE 加密 DEK */
	std::string encrypted_dek;

	bool operator==(const HsecColumn& other) const;
};

/** 存储于 HDFS xattr 的 ABE 元数据安全头 */
struct Hsec {
	std::string file_path;
	std::vector<HsecColumn> abe_headers;

	bool operator==(const Hsec& other) const;
};

/** Driver TEE 派发给 Worker 的动态写能力票据 */
struct TaskTicket {
	std::string job_id;
	std::string task_id;
	std::string worker_ip;
	std::string allowed_target_path;
	int64_t expires_at;
	std::string driver_signature;

	bool operator==(const TaskTicket& other) const;
};

/** 列字节偏移元数据（Worker 读管道动态掩码用，Step 2 消费） */
struct ColumnByteRange {
	std::string column_name;
	uint64_t byte_offset;
	uint64_t byte_length;

	bool operator==(const ColumnByteRange& other) const;
};

} // namespace abe_spark
