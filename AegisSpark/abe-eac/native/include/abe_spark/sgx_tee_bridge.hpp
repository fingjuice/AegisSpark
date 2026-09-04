#pragma once

#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"

#include <memory>
#include <mutex>
#include <string>

namespace abe_spark {

/** Intel SGX Enclave 内 FAME CP-ABE（USK/MSK 不出 TEE） */
class SgxTeeBridge {
public:
	static bool isCompiledIn();
	static bool isAvailable(const CryptoConfig& config);

	static SgxTeeBridge& instance();

	bool ensureLoaded(const CryptoConfig& config);
	void destroy();

	bool initAbe();
	bool issueUser(const std::string& userId, const abe::AttributeSet& attrs);
	DekEncryptResult encryptDek(const std::string& policy, const std::vector<uint8_t>& dek);
	DekDecryptResult decryptDek(
		const std::string& userId,
		const std::string& policy,
		const std::vector<uint8_t>& ctRaw);

private:
	SgxTeeBridge();
	~SgxTeeBridge();

	bool loadEnclave(const std::string& path);

	std::mutex mu_;
	bool loaded_;
	uint64_t eid_;
	std::string enclave_path_;
};

/** tee.mode=sgx 时使用；否则回退进程内 FAME */
std::shared_ptr<ICPABECrypto> createCpabeCrypto(const CryptoConfig& config);

} // namespace abe_spark
