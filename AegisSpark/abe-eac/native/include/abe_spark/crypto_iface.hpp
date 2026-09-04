#pragma once

#include "abe_spark/crypto_config.hpp"
#include "abe_spark/types.hpp"

#include <abe_framework.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace abe_spark {

struct SignResult {
	bool ok;
	std::string signature_hex;
	std::string reason;
};

struct VerifyResult {
	bool ok;
	std::string reason;
};

struct DekEncryptResult {
	bool ok;
	std::string encrypted_dek_base64;
	std::string reason;
};

struct DekDecryptResult {
	bool ok;
	std::vector<uint8_t> dek;
	std::string reason;
};

struct AesGcmResult {
	bool ok;
	std::vector<uint8_t> output;
	std::string reason;
};

/** CP-ABE 包装接口：委托 abe::ABEApi / mcl 双线性配对 */
class ICPABECrypto {
public:
	virtual ~ICPABECrypto() {}

	virtual abe::SetupResult setup() = 0;
	virtual abe::UserSecretKey issueUserAttributes(
		const abe::MasterSecretKey& msk,
		const abe::PublicParams& pp,
		const std::string& userId,
		const abe::AttributeSet& attributes) = 0;
	virtual DekEncryptResult encryptDek(
		const abe::PublicParams& pp,
		const std::vector<uint8_t>& dek,
		const std::string& policyExpression) = 0;
	virtual DekDecryptResult decryptDek(
		const abe::PublicParams& pp,
		const abe::UserSecretKey& usk,
		const std::string& encrypted_dek_base64,
		const std::string& policyExpression) = 0;
	virtual bool authorize(
		const abe::UserSecretKey& usk,
		const std::string& policyExpression) = 0;
};

/** AES-GCM 流加密接口（DataNode 密文块） */
class IAESGCMCrypto {
public:
	virtual ~IAESGCMCrypto() {}

	virtual AesGcmResult encrypt(
		const std::vector<uint8_t>& key,
		const std::vector<uint8_t>& nonce,
		const std::vector<uint8_t>& plaintext) = 0;
	virtual AesGcmResult decrypt(
		const std::vector<uint8_t>& key,
		const std::vector<uint8_t>& nonce,
		const std::vector<uint8_t>& ciphertext_with_tag) = 0;
};

/** ECDSA 签名接口（Admin / Driver TEE 背书） */
class IECDSACrypto {
public:
	virtual ~IECDSACrypto() {}

	virtual SignResult sign(
		const std::vector<uint8_t>& message,
		const std::string& private_key_pem_path) = 0;
	virtual VerifyResult verify(
		const std::vector<uint8_t>& message,
		const std::string& signature_hex,
		const std::string& public_key_pem_path) = 0;
};

/**
 * 系统管理员（PKG / 离散信任锚）高层接口。
 * 对应 Task 1: IssueUserAttributes / EndorseSparkJob
 */
class IKgc {
public:
	virtual ~IKgc() {}

	virtual abe::UserSecretKey issueUserAttributes(
		const std::string& userId,
		const abe::AttributeSet& attributes) = 0;
	virtual AdminEndorsement endorseSparkJob(
		const std::vector<uint8_t>& binary,
		const std::vector<std::string>& allowedDirs,
		int64_t timestamp) = 0;
	virtual const abe::PublicParams& publicParams() const = 0;
};

/** 密码学组件工厂：根据 CryptoConfig 构造具体实现 */
class CryptoProvider {
public:
	explicit CryptoProvider(const CryptoConfig& config);

	std::shared_ptr<ICPABECrypto> cpabe() const;
	std::shared_ptr<IAESGCMCrypto> aesGcm() const;
	std::shared_ptr<IECDSACrypto> ecdsa() const;
	std::shared_ptr<IKgc> kgc() const;

	const CryptoConfig& config() const;

private:
	CryptoConfig config_;
	std::shared_ptr<ICPABECrypto> cpabe_;
	std::shared_ptr<IAESGCMCrypto> aes_gcm_;
	std::shared_ptr<IECDSACrypto> ecdsa_;
	std::shared_ptr<IKgc> kgc_;
};

/** 构建 AdminEndorsement 待签名 canonical payload（确定性序列化） */
std::vector<uint8_t> buildAdminEndorsementPayload(
	const std::string& appCodeHash,
	const std::vector<std::string>& allowedDirs,
	int64_t timestamp);

/** 构建 TaskTicket 待签名 canonical payload */
std::vector<uint8_t> buildTaskTicketPayload(
	const std::string& jobId,
	const std::string& taskId,
	const std::string& workerIp,
	const std::string& allowedTargetPath,
	int64_t expiresAt);

/** 构建 Write Verification 确认指令待签名 canonical payload */
std::vector<uint8_t> buildWriteConfirmPayload(
	const std::string& jobId,
	const std::string& taskId,
	const std::string& workerIp,
	const std::string& targetPath,
	uint64_t payloadBytes,
	int64_t issuedAt);

/** SHA256 hex（app_code_hash 计算） */
std::string sha256Hex(const std::vector<uint8_t>& data);

} // namespace abe_spark
