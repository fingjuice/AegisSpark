#pragma once

#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/types.hpp"
#include "abe_spark/worker_encrypt.hpp"
#include "abe_spark/worker_read.hpp"

#include <abe_framework.hpp>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace abe_spark {

/** JNI / Java 层共享的运行时上下文（单例） */
class AbeRuntime {
public:
	static AbeRuntime& instance();

	bool init(const std::string& configPath);
	bool isInitialized() const;
	void setUserContext(const std::string& userId, const abe::AttributeSet& attributes);

	void setDekCacheEnabled(bool enabled);
	bool dekCacheEnabled() const;
	void clearDekCache();

	void setSharedDekEnabled(bool enabled);
	bool sharedDekEnabled() const;
	SharedDekStore* sharedDekStore();

	WorkerReadResult processWorkerRead(
		const std::vector<uint8_t>& encryptedStream,
		const Hsec& header,
		const std::vector<ColumnByteRange>& columnRanges);

	std::string encryptCompanyTableJson(
		uint64_t companyId,
		uint32_t recordCount,
		const std::string& policyPublic,
		const std::string& policySensitive,
		const std::string& hdfsPathPrefix);

	CryptoProvider* provider();
	const abe::UserSecretKey* userKey() const;

private:
	AbeRuntime();

	mutable std::mutex mutex_;
	bool initialized_;
	std::unique_ptr<CryptoProvider> provider_;
	abe::UserSecretKey user_key_;
	bool has_user_;
	bool dek_cache_enabled_;
	bool shared_dek_enabled_;
	std::unordered_map<std::string, std::vector<uint8_t> > dek_cache_;
	SharedDekStore shared_deks_;
};

} // namespace abe_spark
