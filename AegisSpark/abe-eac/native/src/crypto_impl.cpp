#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/sgx_tee_bridge.hpp"
#include "abe_spark/util_encoding.hpp"

#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include <mcl/fp.hpp>
#include <openssl/evp.h>
#include <openssl/pem.h>

namespace abe_spark {
namespace {

std::vector<uint8_t> serializeAbeCiphertext(const abe::Ciphertext& ct)
{
	std::vector<uint8_t> out;
	const std::string& pol = ct.policyExpr;
	auto appendU32 = [&](uint32_t v) {
		out.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
		out.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
		out.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
		out.push_back(static_cast<uint8_t>(v & 0xff));
	};
	appendU32(static_cast<uint32_t>(pol.size()));
	out.insert(out.end(), pol.begin(), pol.end());
	std::string g1s;
	ct.ephemeral.getStr(g1s, mcl::IoSerialize);
	appendU32(static_cast<uint32_t>(g1s.size()));
	out.insert(out.end(), g1s.begin(), g1s.end());
	appendU32(static_cast<uint32_t>(ct.payload.size()));
	out.insert(out.end(), ct.payload.begin(), ct.payload.end());
	return out;
}

bool deserializeAbeCiphertext(const std::vector<uint8_t>& buf, abe::Ciphertext& ct)
{
	const uint8_t* p = buf.data();
	const uint8_t* end = buf.data() + buf.size();
	auto readU32 = [&](uint32_t& v) -> bool {
		if (end - p < 4) return false;
		v = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
			(static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
		p += 4;
		return true;
	};
	uint32_t n = 0;
	if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) return false;
	ct.policyExpr.assign(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p) + n);
	p += n;
	if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) return false;
	{
		std::string g1s(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p) + n);
		p += n;
		ct.ephemeral.setStr(g1s, mcl::IoSerialize);
	}
	if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) return false;
	ct.payload.assign(p, p + n);
	p += n;
	return p == end;
}

EVP_PKEY* loadPrivateKeyPem(const std::string& path)
{
	if (path.empty()) return NULL;
	BIO* bio = BIO_new_file(path.c_str(), "r");
	if (!bio) return NULL;
	EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
	BIO_free(bio);
	return pkey;
}

EVP_PKEY* loadPublicKeyPem(const std::string& path)
{
	if (path.empty()) return NULL;
	BIO* bio = BIO_new_file(path.c_str(), "r");
	if (!bio) return NULL;
	EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
	BIO_free(bio);
	return pkey;
}

class AESGCMCryptoImpl : public IAESGCMCrypto {
public:
	explicit AESGCMCryptoImpl(const CryptoConfig& config)
		: config_(config)
	{
	}

	AesGcmResult encrypt(
		const std::vector<uint8_t>& key,
		const std::vector<uint8_t>& nonce,
		const std::vector<uint8_t>& plaintext)
	{
		AesGcmResult out;
		if (static_cast<int>(key.size() * 8) != config_.aead_key_bits || static_cast<int>(nonce.size()) != config_.aead_nonce_bytes) {
			out.ok = false;
			out.reason = "invalid key or nonce size";
			return out;
		}
		out.output.resize(plaintext.size() + static_cast<size_t>(config_.aead_tag_bytes));
		EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
		if (!ctx) {
			out.ok = false;
			out.reason = "EVP_CIPHER_CTX_new failed";
			return out;
		}
		int len = 0;
		int ciphertextLen = 0;
		const EVP_CIPHER* cipher = (config_.aead_key_bits == 128) ? EVP_aes_128_gcm() : EVP_aes_256_gcm();
		if (EVP_EncryptInit_ex(ctx, cipher, NULL, NULL, NULL) != 1 ||
			EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(nonce.size()), NULL) != 1 ||
			EVP_EncryptInit_ex(ctx, NULL, NULL, key.data(), nonce.data()) != 1 ||
			EVP_EncryptUpdate(ctx, out.output.data(), &len, plaintext.data(), static_cast<int>(plaintext.size())) != 1) {
			EVP_CIPHER_CTX_free(ctx);
			out.ok = false;
			out.reason = "AES-GCM encrypt failed";
			return out;
		}
		ciphertextLen = len;
		if (EVP_EncryptFinal_ex(ctx, out.output.data() + ciphertextLen, &len) != 1) {
			EVP_CIPHER_CTX_free(ctx);
			out.ok = false;
			out.reason = "AES-GCM encrypt final failed";
			return out;
		}
		ciphertextLen += len;
		if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, config_.aead_tag_bytes, out.output.data() + ciphertextLen) != 1) {
			EVP_CIPHER_CTX_free(ctx);
			out.ok = false;
			out.reason = "AES-GCM get tag failed";
			return out;
		}
		EVP_CIPHER_CTX_free(ctx);
		out.output.resize(static_cast<size_t>(ciphertextLen + config_.aead_tag_bytes));
		out.ok = true;
		return out;
	}

	AesGcmResult decrypt(
		const std::vector<uint8_t>& key,
		const std::vector<uint8_t>& nonce,
		const std::vector<uint8_t>& ciphertext_with_tag)
	{
		AesGcmResult out;
		if (static_cast<int>(key.size() * 8) != config_.aead_key_bits ||
			static_cast<int>(nonce.size()) != config_.aead_nonce_bytes ||
			static_cast<int>(ciphertext_with_tag.size()) < config_.aead_tag_bytes) {
			out.ok = false;
			out.reason = "invalid decrypt input size";
			return out;
		}
		const size_t ctLen = ciphertext_with_tag.size() - static_cast<size_t>(config_.aead_tag_bytes);
		out.output.resize(ctLen);
		EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
		if (!ctx) {
			out.ok = false;
			out.reason = "EVP_CIPHER_CTX_new failed";
			return out;
		}
		int len = 0;
		int plaintextLen = 0;
		const EVP_CIPHER* cipher = (config_.aead_key_bits == 128) ? EVP_aes_128_gcm() : EVP_aes_256_gcm();
		if (EVP_DecryptInit_ex(ctx, cipher, NULL, NULL, NULL) != 1 ||
			EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(nonce.size()), NULL) != 1 ||
			EVP_DecryptInit_ex(ctx, NULL, NULL, key.data(), nonce.data()) != 1 ||
			EVP_DecryptUpdate(ctx, out.output.data(), &len, ciphertext_with_tag.data(), static_cast<int>(ctLen)) != 1) {
			EVP_CIPHER_CTX_free(ctx);
			out.ok = false;
			out.reason = "AES-GCM decrypt failed";
			return out;
		}
		plaintextLen = len;
		if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, config_.aead_tag_bytes,
				const_cast<uint8_t*>(ciphertext_with_tag.data() + ctLen)) != 1 ||
			EVP_DecryptFinal_ex(ctx, out.output.data() + plaintextLen, &len) != 1) {
			EVP_CIPHER_CTX_free(ctx);
			out.ok = false;
			out.reason = "AES-GCM authentication failed";
			return out;
		}
		plaintextLen += len;
		EVP_CIPHER_CTX_free(ctx);
		out.output.resize(static_cast<size_t>(plaintextLen));
		out.ok = true;
		return out;
	}

private:
	CryptoConfig config_;
};

class Ed25519CryptoImpl : public IECDSACrypto {
public:
	SignResult sign(const std::vector<uint8_t>& message, const std::string& private_key_pem_path)
	{
		SignResult out;
		EVP_PKEY* pkey = loadPrivateKeyPem(private_key_pem_path);
		if (!pkey) {
			out.ok = false;
			out.reason = "failed to load private key: " + private_key_pem_path;
			return out;
		}
		if (EVP_PKEY_id(pkey) != EVP_PKEY_ED25519) {
			EVP_PKEY_free(pkey);
			out.ok = false;
			out.reason = "PEM is not an Ed25519 private key";
			return out;
		}
		// Ed25519 is PureEdDSA: one-shot DigestSign with NULL digest (RFC 8032).
		EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
		size_t sigLen = 0;
		std::vector<uint8_t> sig;
		if (!mdctx ||
			EVP_DigestSignInit(mdctx, NULL, NULL, NULL, pkey) != 1 ||
			EVP_DigestSign(mdctx, NULL, &sigLen, message.data(), message.size()) != 1) {
			EVP_MD_CTX_free(mdctx);
			EVP_PKEY_free(pkey);
			out.ok = false;
			out.reason = "Ed25519 sign init failed";
			return out;
		}
		sig.resize(sigLen);
		if (EVP_DigestSign(mdctx, sig.data(), &sigLen, message.data(), message.size()) != 1) {
			EVP_MD_CTX_free(mdctx);
			EVP_PKEY_free(pkey);
			out.ok = false;
			out.reason = "Ed25519 sign final failed";
			return out;
		}
		sig.resize(sigLen);
		EVP_MD_CTX_free(mdctx);
		EVP_PKEY_free(pkey);
		out.signature_hex = bytesToHex(sig);
		out.ok = true;
		return out;
	}

	VerifyResult verify(
		const std::vector<uint8_t>& message,
		const std::string& signature_hex,
		const std::string& public_key_pem_path)
	{
		VerifyResult out;
		EVP_PKEY* pkey = loadPublicKeyPem(public_key_pem_path);
		if (!pkey) {
			out.ok = false;
			out.reason = "failed to load public key: " + public_key_pem_path;
			return out;
		}
		if (EVP_PKEY_id(pkey) != EVP_PKEY_ED25519) {
			EVP_PKEY_free(pkey);
			out.ok = false;
			out.reason = "PEM is not an Ed25519 public key";
			return out;
		}
		std::vector<uint8_t> sig = hexToBytes(signature_hex);
		EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
		if (!mdctx || EVP_DigestVerifyInit(mdctx, NULL, NULL, NULL, pkey) != 1) {
			EVP_MD_CTX_free(mdctx);
			EVP_PKEY_free(pkey);
			out.ok = false;
			out.reason = "Ed25519 verify init failed";
			return out;
		}
		int rc = EVP_DigestVerify(mdctx, sig.data(), sig.size(), message.data(), message.size());
		EVP_MD_CTX_free(mdctx);
		EVP_PKEY_free(pkey);
		out.ok = (rc == 1);
		if (!out.ok) out.reason = "Ed25519 signature verification failed";
		return out;
	}
};

class KgcImpl : public IKgc {
public:
	KgcImpl(
		const CryptoConfig& config,
		std::shared_ptr<ICPABECrypto> cpabe,
		std::shared_ptr<IECDSACrypto> ecdsa)
		: config_(config)
		, cpabe_(cpabe)
		, ecdsa_(ecdsa)
		, setup_(cpabe_->setup())
	{
	}

	abe::UserSecretKey issueUserAttributes(
		const std::string& userId,
		const abe::AttributeSet& attributes)
	{
		return cpabe_->issueUserAttributes(setup_.msk, setup_.pp, userId, attributes);
	}

	AdminEndorsement endorseSparkJob(
		const std::vector<uint8_t>& binary,
		const std::vector<std::string>& allowedDirs,
		int64_t timestamp)
	{
		AdminEndorsement endorsement;
		endorsement.app_code_hash = sha256Hex(binary);
		endorsement.allowed_root_directories = allowedDirs;
		endorsement.timestamp = timestamp;
		std::vector<uint8_t> payload = buildAdminEndorsementPayload(
			endorsement.app_code_hash, allowedDirs, timestamp);
		SignResult sig = ecdsa_->sign(payload, config_.admin_private_key_path);
		if (!sig.ok) {
			throw std::runtime_error("EndorseSparkJob sign failed: " + sig.reason);
		}
		endorsement.admin_signature = sig.signature_hex;
		return endorsement;
	}

	const abe::PublicParams& publicParams() const
	{
		return setup_.pp;
	}

private:
	CryptoConfig config_;
	std::shared_ptr<ICPABECrypto> cpabe_;
	std::shared_ptr<IECDSACrypto> ecdsa_;
	abe::SetupResult setup_;
};

} // namespace

std::vector<uint8_t> buildAdminEndorsementPayload(
	const std::string& appCodeHash,
	const std::vector<std::string>& allowedDirs,
	int64_t timestamp)
{
	std::ostringstream oss;
	oss << appCodeHash << "|";
	for (size_t i = 0; i < allowedDirs.size(); ++i) {
		if (i > 0) oss << ",";
		oss << allowedDirs[i];
	}
	oss << "|" << timestamp;
	const std::string s = oss.str();
	return std::vector<uint8_t>(s.begin(), s.end());
}

std::vector<uint8_t> buildTaskTicketPayload(
	const std::string& jobId,
	const std::string& taskId,
	const std::string& workerIp,
	const std::string& allowedTargetPath,
	int64_t expiresAt)
{
	std::ostringstream oss;
	oss << jobId << "|" << taskId << "|" << workerIp << "|" << allowedTargetPath << "|" << expiresAt;
	const std::string s = oss.str();
	return std::vector<uint8_t>(s.begin(), s.end());
}

std::vector<uint8_t> buildWriteConfirmPayload(
	const std::string& jobId,
	const std::string& taskId,
	const std::string& workerIp,
	const std::string& targetPath,
	uint64_t payloadBytes,
	int64_t issuedAt)
{
	std::ostringstream oss;
	oss << jobId << "|" << taskId << "|" << workerIp << "|" << targetPath << "|"
		<< payloadBytes << "|" << issuedAt;
	const std::string s = oss.str();
	return std::vector<uint8_t>(s.begin(), s.end());
}

std::string sha256Hex(const std::vector<uint8_t>& data)
{
	uint8_t digest[32];
	if (data.empty()) {
		static const uint8_t z = 0;
		mcl::fp::sha256(digest, 32, &z, 1);
	} else {
		mcl::fp::sha256(digest, 32, &data[0], data.size());
	}
	return bytesToHex(std::vector<uint8_t>(digest, digest + 32));
}

CryptoProvider::CryptoProvider(const CryptoConfig& config)
	: config_(config)
{
	cpabe_ = createCpabeCrypto(config_);
	aes_gcm_.reset(new AESGCMCryptoImpl(config_));
	ecdsa_.reset(new Ed25519CryptoImpl());
	kgc_.reset(new KgcImpl(config_, cpabe_, ecdsa_));
}

std::shared_ptr<ICPABECrypto> CryptoProvider::cpabe() const { return cpabe_; }
std::shared_ptr<IAESGCMCrypto> CryptoProvider::aesGcm() const { return aes_gcm_; }
std::shared_ptr<IECDSACrypto> CryptoProvider::ecdsa() const { return ecdsa_; }
std::shared_ptr<IKgc> CryptoProvider::kgc() const { return kgc_; }
const CryptoConfig& CryptoProvider::config() const { return config_; }

} // namespace abe_spark
