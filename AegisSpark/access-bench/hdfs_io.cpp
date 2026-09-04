#include "hdfs_io.hpp"

#include "abe_spark/types_json.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <sstream>

namespace abe_bench {
namespace {

std::string pathBasename(const std::string& path)
{
	const size_t pos = path.find_last_of('/');
	return pos == std::string::npos ? path : path.substr(pos + 1);
}

std::string sidecarPathForEnc(const std::string& enc_path)
{
	const std::string base = hdfsPathFromUri(enc_path);
	if (base.size() > 4 && base.compare(base.size() - 4, 4, ".enc") == 0) {
		return base.substr(0, base.size() - 4) + ".meta.json";
	}
	return base + ".meta.json";
}

} // namespace

std::string parseNamenodeHost(const std::string& hdfs_uri)
{
	const std::string prefix = "hdfs://";
	if (hdfs_uri.compare(0, prefix.size(), prefix) != 0) {
		return hdfs_uri;
	}
	const size_t rest = prefix.size();
	const size_t colon = hdfs_uri.find(':', rest);
	const size_t slash = hdfs_uri.find('/', rest);
	if (colon != std::string::npos && (slash == std::string::npos || colon < slash)) {
		return hdfs_uri.substr(rest, colon - rest);
	}
	if (slash != std::string::npos) {
		return hdfs_uri.substr(rest, slash - rest);
	}
	return hdfs_uri.substr(rest);
}

int parseNamenodePort(const std::string& hdfs_uri, int default_port)
{
	const std::string prefix = "hdfs://";
	if (hdfs_uri.compare(0, prefix.size(), prefix) != 0) {
		return default_port;
	}
	const size_t rest = prefix.size();
	const size_t colon = hdfs_uri.find(':', rest);
	if (colon == std::string::npos) return default_port;
	const size_t slash = hdfs_uri.find('/', colon);
	const std::string port_str = slash == std::string::npos
		? hdfs_uri.substr(colon + 1)
		: hdfs_uri.substr(colon + 1, slash - colon - 1);
	return atoi(port_str.c_str());
}

std::string hdfsPathFromUri(const std::string& hdfs_uri)
{
	const std::string prefix = "hdfs://";
	if (hdfs_uri.compare(0, prefix.size(), prefix) != 0) {
		return hdfs_uri;
	}
	const size_t rest = prefix.size();
	const size_t slash = hdfs_uri.find('/', rest);
	if (slash == std::string::npos) return "/";
	return hdfs_uri.substr(slash);
}

HdfsClient::HdfsClient(const std::string& namenode_uri, const std::string& user)
	: namenode_uri_(namenode_uri)
	, user_(user)
	, fs_(NULL)
{
}

HdfsClient::~HdfsClient()
{
	if (fs_ != NULL) {
		hdfsDisconnect(fs_);
		fs_ = NULL;
	}
}

bool HdfsClient::connect()
{
	if (fs_ != NULL) return true;
	const std::string host = parseNamenodeHost(namenode_uri_);
	const int port = parseNamenodePort(namenode_uri_, 9000);
	fs_ = hdfsConnectAsUser(host.c_str(), port, user_.c_str());
	return fs_ != NULL;
}

bool HdfsClient::mkdirs(const std::string& path)
{
	if (!connect()) return false;
	return hdfsCreateDirectory(fs_, hdfsPathFromUri(path).c_str()) == 0;
}

bool HdfsClient::exists(const std::string& path)
{
	if (!connect()) return false;
	return hdfsExists(fs_, hdfsPathFromUri(path).c_str()) == 0;
}

std::vector<std::string> HdfsClient::listEncFiles(const std::string& dir)
{
	std::vector<std::string> out;
	if (!connect()) return out;
	int numEntries = 0;
	const std::string hpath = hdfsPathFromUri(dir);
	hdfsFileInfo* infos = hdfsListDirectory(fs_, hpath.c_str(), &numEntries);
	if (infos == NULL) return out;
	for (int i = 0; i < numEntries; ++i) {
		const std::string name = pathBasename(infos[i].mName);
		if (name.size() > 4 && name.compare(name.size() - 4, 4, ".enc") == 0) {
			out.push_back(infos[i].mName);
		}
	}
	hdfsFreeFileInfo(infos, numEntries);
	std::sort(out.begin(), out.end());
	return out;
}

HdfsReadResult HdfsClient::readFile(const std::string& path)
{
	HdfsReadResult out;
	out.ok = false;
	if (!connect()) {
		out.reason = "hdfs not connected";
		return out;
	}
	hdfsFile file = hdfsOpenFile(fs_, hdfsPathFromUri(path).c_str(), O_RDONLY, 0, 0, 0);
	if (file == NULL) {
		out.reason = "hdfsOpenFile failed";
		return out;
	}
	std::vector<uint8_t> buf;
	char chunk[65536];
	for (;;) {
		const tSize n = hdfsRead(fs_, file, chunk, sizeof(chunk));
		if (n < 0) {
			hdfsCloseFile(fs_, file);
			out.reason = "hdfsRead failed";
			return out;
		}
		if (n == 0) break;
		buf.insert(buf.end(), chunk, chunk + n);
	}
	hdfsCloseFile(fs_, file);
	out.ok = true;
	out.data.swap(buf);
	return out;
}

bool HdfsClient::writeFile(const std::string& path, const std::vector<uint8_t>& data)
{
	if (!connect()) return false;
	hdfsFile file = hdfsOpenFile(fs_, hdfsPathFromUri(path).c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
	if (file == NULL) return false;
	if (!data.empty()) {
		const tSize written = hdfsWrite(fs_, file,
			reinterpret_cast<const char*>(&data[0]),
			static_cast<tSize>(data.size()));
		if (written < 0 || static_cast<size_t>(written) != data.size()) {
			hdfsCloseFile(fs_, file);
			return false;
		}
	}
	hdfsCloseFile(fs_, file);
	return true;
}

bool HdfsClient::setXAttr(const std::string& path, const std::string& name, const std::string& value)
{
	(void)path;
	(void)name;
	(void)value;
	return false;
}

bool HdfsClient::getXAttr(const std::string& path, const std::string& name, std::string& out)
{
	(void)path;
	(void)name;
	out.clear();
	return false;
}

bool HdfsClient::setTableMeta(
	const std::string& path,
	const abe_spark::Hsec& header,
	const std::vector<abe_spark::ColumnByteRange>& ranges)
{
	const std::string sidecar = sidecarPathForEnc(path);
	std::ostringstream oss;
	oss << "{\"header\":" << abe_spark::TypesJson::toJson(header)
		<< ",\"column_layout\":" << abe_spark::TypesJson::toJsonArray(ranges) << "}";
	const std::string json = oss.str();
	std::vector<uint8_t> bytes(json.begin(), json.end());
	return writeFile(sidecar, bytes);
}

bool HdfsClient::writeEncryptedTable(
	const std::string& path,
	const std::vector<uint8_t>& ciphertext,
	const abe_spark::Hsec& header,
	const std::vector<abe_spark::ColumnByteRange>& ranges)
{
	if (!writeFile(path, ciphertext)) return false;
	return setTableMeta(path, header, ranges);
}

bool HdfsClient::readTableMeta(const std::string& path, HdfsTableMeta& out)
{
	out.file_path = path;
	const std::string sidecar = sidecarPathForEnc(path);
	HdfsReadResult meta_file = readFile(sidecar);
	if (!meta_file.ok) return false;
	const std::string json(meta_file.data.begin(), meta_file.data.end());

	const std::string header_key = "\"header\":";
	const std::string layout_key = "\"column_layout\":";
	const size_t hp = json.find(header_key);
	const size_t lp = json.find(layout_key);
	if (hp == std::string::npos || lp == std::string::npos) return false;

	size_t hstart = hp + header_key.size();
	while (hstart < json.size() && json[hstart] != '{') ++hstart;
	int depth = 0;
	size_t hend = hstart;
	for (; hend < json.size(); ++hend) {
		if (json[hend] == '{') ++depth;
		else if (json[hend] == '}') {
			--depth;
			if (depth == 0) {
				++hend;
				break;
			}
		}
	}

	size_t lstart = lp + layout_key.size();
	while (lstart < json.size() && json[lstart] != '[') ++lstart;
	depth = 0;
	size_t lend = lstart;
	for (; lend < json.size(); ++lend) {
		if (json[lend] == '[') ++depth;
		else if (json[lend] == ']') {
			--depth;
			if (depth == 0) {
				++lend;
				break;
			}
		}
	}

	try {
		out.header = abe_spark::TypesJson::hsecFromJson(
			json.substr(hstart, hend - hstart));
		out.ranges = abe_spark::TypesJson::columnByteRangesFromJson(
			json.substr(lstart, lend - lstart));
	} catch (const abe_spark::JsonParseError&) {
		return false;
	}

	uint64_t plain = 0;
	for (size_t i = 0; i < out.ranges.size(); ++i) {
		plain += out.ranges[i].byte_length;
	}
	out.plain_bytes = plain;
	return true;
}

HdfsDirectWriter::HdfsDirectWriter(HdfsClient& client) : client_(client) {}

abe_spark::WorkerWriteResult HdfsDirectWriter::write(
	const std::string& target_path,
	const std::vector<uint8_t>& encrypted_data)
{
	abe_spark::WorkerWriteResult out;
	out.ok = false;
	out.bytes_written = 0;
	if (!client_.writeFile(target_path, encrypted_data)) {
		out.reason = "hdfs write failed";
		return out;
	}
	out.ok = true;
	out.bytes_written = encrypted_data.size();
	return out;
}

} // namespace abe_bench
