#include "abe_spark_ffi.h"

#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/driver_tee.hpp"
#include "abe_spark/write_verification_tee.hpp"
#include "abe_spark/runtime.hpp"
#include "abe_spark/types_json.hpp"
#include "abe_spark/util_encoding.hpp"
#include "abe_spark/worker_encrypt.hpp"

#include <abe_framework.hpp>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace {

thread_local std::string g_last_error;

void setError(const std::string& msg)
{
	g_last_error = msg;
}

char* dupCString(const std::string& s)
{
	char* out = static_cast<char*>(std::malloc(s.size() + 1));
	if (!out) {
		setError("malloc failed");
		return nullptr;
	}
	std::memcpy(out, s.c_str(), s.size() + 1);
	return out;
}

std::vector<uint8_t> readFileBytes(const std::string& path)
{
	std::ifstream ifs(path.c_str(), std::ios::binary);
	if (!ifs) {
		throw std::runtime_error("cannot read file: " + path);
	}
	return std::vector<uint8_t>(
		std::istreambuf_iterator<char>(ifs),
		std::istreambuf_iterator<char>());
}

abe::AttributeSet attrsFromJsonArray(const std::string& json)
{
	abe::AttributeSet attrs;
	size_t pos = 0;
	size_t i = 0;
	while (i < json.size() && json[i] != '[') ++i;
	if (i >= json.size()) return attrs;
	pos = i + 1;
	while (pos < json.size()) {
		while (pos < json.size() && (json[pos] == ' ' || json[pos] == ',')) ++pos;
		if (pos < json.size() && json[pos] == ']') break;
		if (pos >= json.size() || json[pos] != '"') break;
		++pos;
		size_t start = pos;
		while (pos < json.size() && json[pos] != '"') ++pos;
		attrs.insert(json.substr(start, pos - start));
		if (pos < json.size() && json[pos] == '"') ++pos;
	}
	return attrs;
}

std::vector<std::string> stringArrayFromJson(const std::string& json)
{
	std::vector<std::string> out;
	size_t pos = 0;
	size_t i = 0;
	while (i < json.size() && json[i] != '[') ++i;
	if (i >= json.size()) return out;
	pos = i + 1;
	while (pos < json.size()) {
		while (pos < json.size() && (json[pos] == ' ' || json[pos] == ',')) ++pos;
		if (pos < json.size() && json[pos] == ']') break;
		if (pos >= json.size() || json[pos] != '"') break;
		++pos;
		size_t start = pos;
		while (pos < json.size() && json[pos] != '"') ++pos;
		out.push_back(json.substr(start, pos - start));
		if (pos < json.size() && json[pos] == '"') ++pos;
	}
	return out;
}

struct GlobalState {
	std::mutex mutex;
	std::string config_path;
	std::unique_ptr<abe_spark::CryptoProvider> provider;
	std::unique_ptr<abe_spark::DriverTeeOrchestrator> driver;
	std::unique_ptr<abe_spark::WriteVerificationTee> gatekeeper;
	abe_spark::CryptoConfig config;
	bool initialized = false;
};

GlobalState& state()
{
	static GlobalState s;
	return s;
}

bool ensureInit()
{
	return state().initialized && state().provider;
}

bool loadRuntime(const std::string& configPath)
{
	std::lock_guard<std::mutex> lock(state().mutex);
	try {
		state().config_path = configPath;
		state().config = abe_spark::CryptoConfigLoader::loadFromFile(configPath);
		abe_spark::AbeRuntime::instance().init(configPath);
		state().provider.reset(new abe_spark::CryptoProvider(state().config));
		state().driver.reset(new abe_spark::DriverTeeOrchestrator(
			state().provider->ecdsa(), state().config.driver_private_key_path));
		state().gatekeeper.reset(new abe_spark::WriteVerificationTee(
			state().provider->ecdsa(),
			state().config.admin_public_key_path,
			state().config.driver_public_key_path,
			state().config.write_verification_private_key_path,
			state().config.write_verification_public_key_path));
		state().initialized = true;
		return true;
	} catch (const std::exception& ex) {
		setError(ex.what());
		state().initialized = false;
		return false;
	}
}

class FileWriteProxy {
public:
	explicit FileWriteProxy(const std::string& path) : path_(path) {}

	bool write(const std::string& target_path, const std::vector<uint8_t>& encrypted_data)
	{
		(void)target_path;
		std::ofstream ofs(path_.c_str(), std::ios::binary);
		if (!ofs) return false;
		if (!encrypted_data.empty()) {
			ofs.write(reinterpret_cast<const char*>(&encrypted_data[0]),
				static_cast<std::streamsize>(encrypted_data.size()));
		}
		return true;
	}

private:
	std::string path_;
};

} // namespace

extern "C" {

int abe_spark_init(const char* config_path)
{
	if (!config_path) {
		setError("config_path is null");
		return 0;
	}
	return loadRuntime(config_path) ? 1 : 0;
}

int abe_spark_set_user_context(const char* user_id, const char* attributes_json)
{
	if (!user_id || !attributes_json) {
		setError("null argument");
		return 0;
	}
	try {
		if (!abe_spark::AbeRuntime::instance().isInitialized()) {
			abe_spark::AbeRuntime::instance().init(state().config_path);
		}
		abe_spark::AbeRuntime::instance().setUserContext(
			user_id, attrsFromJsonArray(attributes_json));
		return 1;
	} catch (const std::exception& ex) {
		setError(ex.what());
		return 0;
	}
}

void abe_spark_free(void* ptr)
{
	std::free(ptr);
}

const char* abe_spark_last_error(void)
{
	return g_last_error.c_str();
}

char* abe_spark_issue_user_attributes(const char* user_id, const char* attributes_json)
{
	if (!ensureInit() || !user_id || !attributes_json) {
		setError("not initialized or null argument");
		return nullptr;
	}
	try {
		abe::UserSecretKey sk = state().provider->kgc()->issueUserAttributes(
			user_id, attrsFromJsonArray(attributes_json));
		std::ostringstream oss;
		oss << "{\"ok\":true,\"user_id\":\"" << user_id << "\""
		    << ",\"attribute_count\":" << sk.attributes.size() << "}";
		return dupCString(oss.str());
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_endorse_spark_job(
	const char* binary_path,
	const char* allowed_dirs_json,
	int64_t timestamp)
{
	if (!ensureInit() || !binary_path || !allowed_dirs_json) {
		setError("not initialized or null argument");
		return nullptr;
	}
	try {
		std::vector<uint8_t> binary = readFileBytes(binary_path);
		abe_spark::AdminEndorsement e = state().provider->kgc()->endorseSparkJob(
			binary, stringArrayFromJson(allowed_dirs_json), timestamp);
		return dupCString(abe_spark::TypesJson::toJson(e));
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_driver_attestation(const char* eac_url, const char* endorsement_json)
{
	if (!ensureInit() || !eac_url || !endorsement_json) {
		setError("not initialized or null argument");
		return nullptr;
	}
	try {
		abe_spark::AdminEndorsement e =
			abe_spark::TypesJson::adminEndorsementFromJson(endorsement_json);
		abe_spark::AttestationResult r = state().driver->attestationHandshake(eac_url, e);
		std::ostringstream oss;
		oss << "{\"ok\":" << (r.ok ? "true" : "false")
		    << ",\"session_token\":\"" << r.session_token << "\""
		    << ",\"reason\":\"" << r.reason << "\"}";
		return dupCString(oss.str());
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_derive_task_ticket(
	const char* job_id,
	const char* task_id,
	const char* worker_ip,
	const char* allowed_target_path,
	int64_t expires_at)
{
	if (!ensureInit()) {
		setError("not initialized");
		return nullptr;
	}
	try {
		abe_spark::TaskTicket t = state().driver->deriveTaskTicket(
			job_id, task_id, worker_ip, allowed_target_path, expires_at);
		return dupCString(abe_spark::TypesJson::toJson(t));
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_verify_write_chain(
	const char* ticket_json,
	const char* endorsement_json,
	const char* target_path,
	int64_t now_epoch_sec)
{
	if (!ensureInit()) {
		setError("not initialized");
		return nullptr;
	}
	try {
		abe_spark::TaskTicket ticket = abe_spark::TypesJson::taskTicketFromJson(ticket_json);
		abe_spark::AdminEndorsement endorsement =
			abe_spark::TypesJson::adminEndorsementFromJson(endorsement_json);
		abe_spark::WriteVerifyResult r = state().gatekeeper->verifyWriteChain(
			ticket, endorsement, target_path, now_epoch_sec);
		std::ostringstream oss;
		oss << "{\"ok\":" << (r.ok ? "true" : "false")
		    << ",\"reason\":\"" << r.reason << "\"}";
		return dupCString(oss.str());
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_proxy_write(
	const char* ticket_json,
	const char* endorsement_json,
	const char* target_path,
	const unsigned char* encrypted_data,
	size_t encrypted_len,
	int64_t now_epoch_sec,
	const char* output_path)
{
	if (!ensureInit() || !output_path) {
		setError("not initialized or null output_path");
		return nullptr;
	}
	try {
		abe_spark::TaskTicket ticket = abe_spark::TypesJson::taskTicketFromJson(ticket_json);
		abe_spark::AdminEndorsement endorsement =
			abe_spark::TypesJson::adminEndorsementFromJson(endorsement_json);
		std::vector<uint8_t> data;
		if (encrypted_data && encrypted_len > 0) {
			data.assign(encrypted_data, encrypted_data + encrypted_len);
		}
		FileWriteProxy proxy(output_path);
		abe_spark::WriteConfirmInstruction confirm = state().gatekeeper->issueWriteConfirm(
			ticket, endorsement, target_path,
			static_cast<uint64_t>(data.size()), now_epoch_sec);
		if (!confirm.ok) {
			std::ostringstream oss;
			oss << "{\"ok\":false,\"bytes_written\":0,\"monotonic_counter\":0"
			    << ",\"output_path\":\"" << output_path << "\""
			    << ",\"reason\":\"" << confirm.reason << "\"}";
			return dupCString(oss.str());
		}
		if (!proxy.write(target_path, data)) {
			std::ostringstream oss;
			oss << "{\"ok\":false,\"bytes_written\":0,\"monotonic_counter\":0"
			    << ",\"output_path\":\"" << output_path << "\""
			    << ",\"reason\":\"cannot open output: " << output_path << "\"}";
			return dupCString(oss.str());
		}
		std::ostringstream oss;
		oss << "{\"ok\":true,\"bytes_written\":" << data.size()
		    << ",\"monotonic_counter\":0"
		    << ",\"output_path\":\"" << output_path << "\""
		    << ",\"reason\":\"write confirm issued\"}";
		return dupCString(oss.str());
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_process_worker_read(
	const unsigned char* encrypted_stream,
	size_t encrypted_len,
	const char* header_json,
	const char* column_ranges_json)
{
	if (!encrypted_stream || !header_json || !column_ranges_json) {
		setError("null argument");
		return nullptr;
	}
	try {
		std::vector<uint8_t> enc(encrypted_stream, encrypted_stream + encrypted_len);
		abe_spark::Hsec header =
			abe_spark::TypesJson::hsecFromJson(header_json);
		std::vector<abe_spark::ColumnByteRange> ranges =
			abe_spark::TypesJson::columnByteRangesFromJson(column_ranges_json);
		abe_spark::WorkerReadResult result =
			abe_spark::AbeRuntime::instance().processWorkerRead(enc, header, ranges);
		std::ostringstream oss;
		oss << "{\"ok\":" << (result.ok ? "true" : "false")
		    << ",\"plaintext_b64\":\"" << abe_spark::bytesToBase64(result.plaintext) << "\""
		    << ",\"columns_authorized\":" << result.columns_authorized
		    << ",\"columns_masked\":" << result.columns_masked
		    << ",\"reason\":\"" << result.reason << "\"}";
		return dupCString(oss.str());
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

char* abe_spark_encrypt_demo_table(
	const char* policy_public,
	const char* policy_sensitive,
	const char* output_prefix)
{
	if (!ensureInit()) {
		setError("not initialized");
		return nullptr;
	}
	try {
		std::string json = abe_spark::AbeRuntime::instance().encryptCompanyTableJson(
			1, 10,
			policy_public ? policy_public : "role:analyst",
			policy_sensitive ? policy_sensitive : "(role:admin and clearance:5)",
			output_prefix ? output_prefix : "/tmp/sgx-pyspark");
		return dupCString(json);
	} catch (const std::exception& ex) {
		setError(ex.what());
		return nullptr;
	}
}

} // extern "C"
