#pragma once

#include "abe_spark/types.hpp"
#include "abe_spark/worker_write.hpp"

#include <hdfs.h>
#include <cstdint>
#include <string>
#include <vector>

namespace abe_bench {

struct HdfsReadResult {
	bool ok;
	std::vector<uint8_t> data;
	std::string reason;
};

struct HdfsTableMeta {
	std::string file_path;
	abe_spark::Hsec header;
	std::vector<abe_spark::ColumnByteRange> ranges;
	uint64_t plain_bytes;
};

/** libhdfs 客户端：读写密文与 ABE xattr 元数据 */
class HdfsClient {
public:
	HdfsClient(const std::string& namenode_uri, const std::string& user);
	~HdfsClient();

	bool connect();
	bool isConnected() const { return fs_ != NULL; }

	bool mkdirs(const std::string& path);
	bool exists(const std::string& path);
	std::vector<std::string> listEncFiles(const std::string& dir);

	HdfsReadResult readFile(const std::string& path);
	bool writeFile(const std::string& path, const std::vector<uint8_t>& data);
	bool setTableMeta(
		const std::string& path,
		const abe_spark::Hsec& header,
		const std::vector<abe_spark::ColumnByteRange>& ranges);
	bool writeEncryptedTable(
		const std::string& path,
		const std::vector<uint8_t>& ciphertext,
		const abe_spark::Hsec& header,
		const std::vector<abe_spark::ColumnByteRange>& ranges);

	bool readTableMeta(const std::string& path, HdfsTableMeta& out);

private:
	std::string namenode_uri_;
	std::string user_;
	hdfsFS fs_;

	bool setXAttr(const std::string& path, const std::string& name, const std::string& value);
	bool getXAttr(const std::string& path, const std::string& name, std::string& out);
};

/** WorkerWritePipeline 使用的 HDFS 直写实现 */
class HdfsDirectWriter : public abe_spark::IHdfsDirectWriter {
public:
	explicit HdfsDirectWriter(HdfsClient& client);
	abe_spark::WorkerWriteResult write(
		const std::string& target_path,
		const std::vector<uint8_t>& encrypted_data) override;

private:
	HdfsClient& client_;
};

std::string parseNamenodeHost(const std::string& hdfs_uri);
int parseNamenodePort(const std::string& hdfs_uri, int default_port);
/** hdfs://host:port/path -> /path */
std::string hdfsPathFromUri(const std::string& hdfs_uri);

} // namespace abe_bench
