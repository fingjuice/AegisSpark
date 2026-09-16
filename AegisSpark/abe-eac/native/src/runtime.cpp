#include "abe_spark/runtime.hpp"
#include "abe_spark/crypto_config.hpp"
#include "abe_spark/types_json.hpp"
#include "abe_spark/util_encoding.hpp"
#include "abe_spark/worker_encrypt.hpp"

#include <sstream>

namespace abe_spark {

AbeRuntime::AbeRuntime()
	: initialized_(false)
	, has_user_(false)
	, dek_cache_enabled_(false)
	, shared_dek_enabled_(false)
{
}

AbeRuntime& AbeRuntime::instance()
{
	static AbeRuntime s;
	return s;
}

bool AbeRuntime::init(const std::string& configPath)
{
	std::lock_guard<std::mutex> lock(mutex_);
	try {
		CryptoConfig cfg = CryptoConfigLoader::loadFromFile(configPath);
		provider_.reset(new CryptoProvider(cfg));
		initialized_ = true;
		has_user_ = false;
		dek_cache_.clear();
		return true;
	} catch (...) {
		initialized_ = false;
		return false;
	}
}

bool AbeRuntime::isInitialized() const
{
	std::lock_guard<std::mutex> lock(mutex_);
	return initialized_;
}

void AbeRuntime::setUserContext(const std::string& userId, const abe::AttributeSet& attributes)
{
	std::lock_guard<std::mutex> lock(mutex_);
	if (!initialized_ || !provider_) {
		throw std::runtime_error("AbeRuntime not initialized");
	}
	user_key_ = provider_->kgc()->issueUserAttributes(userId, attributes);
	has_user_ = true;
	dek_cache_.clear();
}

void AbeRuntime::setDekCacheEnabled(bool enabled)
{
	std::lock_guard<std::mutex> lock(mutex_);
	dek_cache_enabled_ = enabled;
	if (!enabled) dek_cache_.clear();
}

bool AbeRuntime::dekCacheEnabled() const
{
	std::lock_guard<std::mutex> lock(mutex_);
	return dek_cache_enabled_;
}

void AbeRuntime::clearDekCache()
{
	std::lock_guard<std::mutex> lock(mutex_);
	dek_cache_.clear();
}

void AbeRuntime::setSharedDekEnabled(bool enabled)
{
	std::lock_guard<std::mutex> lock(mutex_);
	shared_dek_enabled_ = enabled;
	if (!enabled) {
		std::lock_guard<std::mutex> slock(shared_deks_.mu);
		shared_deks_.by_policy.clear();
	}
}

bool AbeRuntime::sharedDekEnabled() const
{
	std::lock_guard<std::mutex> lock(mutex_);
	return shared_dek_enabled_;
}

SharedDekStore* AbeRuntime::sharedDekStore()
{
	return shared_dek_enabled_ ? &shared_deks_ : nullptr;
}

WorkerReadResult AbeRuntime::processWorkerRead(
	const std::vector<uint8_t>& encryptedStream,
	const Hsec& header,
	const std::vector<ColumnByteRange>& columnRanges)
{
	std::lock_guard<std::mutex> lock(mutex_);
	if (!initialized_ || !provider_ || !has_user_) {
		WorkerReadResult out;
		out.ok = false;
		out.reason = "AbeRuntime not ready (init or user context missing)";
		return out;
	}

	size_t abeCalls = 0;
	size_t abeHits = 0;
	double abeMs = 0.0;
	std::unordered_map<std::string, std::vector<uint8_t> >* cachePtr =
		dek_cache_enabled_ ? &dek_cache_ : nullptr;
	std::vector<ColumnAccessPlan> plan = WorkerReadPipeline::buildAccessPlan(
		header, columnRanges, *provider_->cpabe(),
		provider_->kgc()->publicParams(), user_key_,
		&abeCalls, &abeMs, cachePtr, &abeHits);

	WorkerReadResult result = WorkerReadPipeline::processStream(
		encryptedStream, plan, *provider_->aesGcm(),
		provider_->config().aead_nonce_bytes,
		provider_->config().aead_tag_bytes);
	result.abe_decrypt_calls = abeCalls;
	result.abe_decrypt_ms = abeMs;
	result.abe_cache_hits = abeHits;
	return result;
}

std::string AbeRuntime::encryptCompanyTableJson(
	uint64_t companyId,
	uint32_t recordCount,
	const std::string& policyPublic,
	const std::string& policySensitive,
	const std::string& hdfsPathPrefix)
{
	std::lock_guard<std::mutex> lock(mutex_);
	if (!initialized_ || !provider_) {
		return "{\"ok\":false,\"reason\":\"AbeRuntime not initialized\"}";
	}
	SharedDekStore* shared = shared_dek_enabled_ ? &shared_deks_ : nullptr;
	CompanyEncryptResult er = encryptCompanyTable(
		*provider_, companyId, recordCount, policyPublic, policySensitive, hdfsPathPrefix, shared);
	if (!er.ok) {
		return std::string("{\"ok\":false,\"reason\":\"") + er.reason + "\"}";
	}
	std::ostringstream oss;
	oss << "{\"ok\":true"
	    << ",\"table_name\":\"" << er.table_name << "\""
	    << ",\"record_count\":" << er.record_count
	    << ",\"plain_bytes\":" << er.plain_bytes
	    << ",\"enc_bytes\":" << er.enc_bytes
	    << ",\"abe_encrypt_ms\":" << er.abe_encrypt_ms
	    << ",\"abe_encrypt_calls\":" << er.abe_encrypt_calls
	    << ",\"aes_encrypt_ms\":" << er.aes_encrypt_ms
	    << ",\"ciphertext_b64\":\"" << bytesToBase64(er.ciphertext) << "\""
	    << ",\"header\":" << TypesJson::toJson(er.header)
	    << ",\"column_layout\":" << TypesJson::toJsonArray(er.ranges)
	    << "}";
	return oss.str();
}

CryptoProvider* AbeRuntime::provider()
{
	return provider_.get();
}

const abe::UserSecretKey* AbeRuntime::userKey() const
{
	return has_user_ ? &user_key_ : NULL;
}

} // namespace abe_spark
