#include "abe_spark/sgx_tee_bridge.hpp"
#include "abe_spark/util_encoding.hpp"

#include <ctime>
#include <cstring>
#include <sstream>
#include <stdexcept>

#ifdef ABE_SPARK_SGX_ENABLED
#include <sgx_urts.h>
#include "Enclave_u.h"

extern "C" void ocall_print_string(const char* str)
{
	if (str) fputs(str, stdout);
}

extern "C" void ocall_clock_gettime_ns(uint64_t* ns_out)
{
	if (!ns_out) return;
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		*ns_out = 0;
		return;
	}
	*ns_out = static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}
#endif

namespace abe_spark {
namespace {

std::string attrsToCsv(const abe::AttributeSet& attrs)
{
	std::ostringstream oss;
	bool first = true;
	for (abe::AttributeSet::const_iterator it = attrs.begin(); it != attrs.end(); ++it) {
		if (!first) oss << ',';
		first = false;
		oss << *it;
	}
	return oss.str();
}

class InProcessCpabeCrypto : public ICPABECrypto {
public:
	explicit InProcessCpabeCrypto(const CryptoConfig& config)
		: config_(config)
	{
		(void)abe::ABEApi::currentEngine();
		if (config_.abe_engine != abe::ABEApi::currentEngine()) {
			abe::ABEApi::useEngine(config_.abe_engine);
		}
	}

	abe::SetupResult setup() override
	{
		abe::SetupResult result = abe::ABEApi::setup(config_.toAbeSetupConfig());
		result.pp.schemeId = config_.scheme_id;
		return result;
	}

	abe::UserSecretKey issueUserAttributes(
		const abe::MasterSecretKey& msk,
		const abe::PublicParams& pp,
		const std::string& userId,
		const abe::AttributeSet& attributes) override
	{
		return abe::ABEApi::keyGen(msk, pp, userId, attributes);
	}

	DekEncryptResult encryptDek(
		const abe::PublicParams& pp,
		const std::vector<uint8_t>& dek,
		const std::string& policyExpression) override
	{
	DekEncryptResult out;
	out.ok = false;
	try {
			abe::Ciphertext ct = abe::ABEApi::encrypt(pp, dek, policyExpression);
			std::vector<uint8_t> raw;
			const std::string& pol = ct.policyExpr;
			auto appendU32 = [&](uint32_t v) {
				raw.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
				raw.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
				raw.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
				raw.push_back(static_cast<uint8_t>(v & 0xff));
			};
			appendU32(static_cast<uint32_t>(pol.size()));
			raw.insert(raw.end(), pol.begin(), pol.end());
			std::string g1s;
			ct.ephemeral.getStr(g1s, mcl::IoSerialize);
			appendU32(static_cast<uint32_t>(g1s.size()));
			raw.insert(raw.end(), g1s.begin(), g1s.end());
			appendU32(static_cast<uint32_t>(ct.payload.size()));
			raw.insert(raw.end(), ct.payload.begin(), ct.payload.end());
			out.encrypted_dek_base64 = bytesToBase64(raw);
			out.ok = true;
		} catch (std::exception& e) {
			out.ok = false;
			out.reason = e.what();
		}
		return out;
	}

	DekDecryptResult decryptDek(
		const abe::PublicParams& pp,
		const abe::UserSecretKey& usk,
		const std::string& encrypted_dek_base64,
		const std::string& policyExpression) override
	{
	DekDecryptResult out;
	out.ok = false;
	try {
			std::vector<uint8_t> raw = base64ToBytes(encrypted_dek_base64);
			abe::Ciphertext ct;
			const uint8_t* p = raw.data();
			const uint8_t* end = raw.data() + raw.size();
			auto readU32 = [&](uint32_t& v) -> bool {
				if (end - p < 4) return false;
				v = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
					(static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
				p += 4;
				return true;
			};
			uint32_t n = 0;
			if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) {
				out.ok = false;
				out.reason = "invalid encrypted_dek blob";
				return out;
			}
			ct.policyExpr.assign(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p) + n);
			p += n;
			if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) {
				out.ok = false;
				out.reason = "invalid encrypted_dek blob";
				return out;
			}
			{
				std::string g1s(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p) + n);
				p += n;
				ct.ephemeral.setStr(g1s, mcl::IoSerialize);
			}
			if (!readU32(n) || end - p < static_cast<std::ptrdiff_t>(n)) {
				out.ok = false;
				out.reason = "invalid encrypted_dek blob";
				return out;
			}
			ct.payload.assign(p, p + n);
			if (ct.policyExpr != policyExpression) {
				out.ok = false;
				out.reason = "policy_expression mismatch";
				return out;
			}
			abe::DecryptResult dr = abe::ABEApi::decrypt(pp, usk, ct);
			out.ok = dr.ok;
			out.dek = dr.plaintext;
			out.reason = dr.reason;
		} catch (std::exception& e) {
			out.ok = false;
			out.reason = e.what();
		}
		return out;
	}

	bool authorize(const abe::UserSecretKey& usk, const std::string& policyExpression) override
	{
		return abe::ABEApi::authorize(usk, policyExpression);
	}

private:
	CryptoConfig config_;
};

class SgxTeeCpabeCrypto : public ICPABECrypto {
public:
	explicit SgxTeeCpabeCrypto(const CryptoConfig& config)
		: config_(config)
	{
		if (!SgxTeeBridge::instance().ensureLoaded(config)) {
			throw std::runtime_error("SGX TEE enclave load failed");
		}
		if (!SgxTeeBridge::instance().initAbe()) {
			throw std::runtime_error("SGX TEE abe init failed");
		}
	}

	abe::SetupResult setup() override
	{
		abe::SetupResult result;
		result.pp.schemeId = config_.scheme_id;
		result.pp.curveParam = config_.curveParam();
		return result;
	}

	abe::UserSecretKey issueUserAttributes(
		const abe::MasterSecretKey&,
		const abe::PublicParams&,
		const std::string& userId,
		const abe::AttributeSet& attributes) override
	{
		if (!SgxTeeBridge::instance().issueUser(userId, attributes)) {
			throw std::runtime_error("SGX TEE issueUser failed");
		}
		abe::UserSecretKey usk;
		usk.userId = userId;
		usk.attributes = attributes;
		return usk;
	}

	DekEncryptResult encryptDek(
		const abe::PublicParams&,
		const std::vector<uint8_t>& dek,
		const std::string& policyExpression) override
	{
		DekEncryptResult out;
		std::vector<uint8_t> raw;
		DekEncryptResult tee = SgxTeeBridge::instance().encryptDek(policyExpression, dek);
		if (!tee.ok) return tee;
		out = tee;
		return out;
	}

	DekDecryptResult decryptDek(
		const abe::PublicParams&,
		const abe::UserSecretKey& usk,
		const std::string& encrypted_dek_base64,
		const std::string& policyExpression) override
	{
		std::vector<uint8_t> raw = base64ToBytes(encrypted_dek_base64);
		return SgxTeeBridge::instance().decryptDek(usk.userId, policyExpression, raw);
	}

	bool authorize(const abe::UserSecretKey& usk, const std::string& policyExpression) override
	{
		// 授权判定：尝试解密 1 字节探测（TEE 内完整策略评估）
		const std::vector<uint8_t> probe(1, 0);
		DekEncryptResult enc = SgxTeeBridge::instance().encryptDek(policyExpression, probe);
		if (!enc.ok) return false;
		std::vector<uint8_t> raw = base64ToBytes(enc.encrypted_dek_base64);
		DekDecryptResult dec = SgxTeeBridge::instance().decryptDek(usk.userId, policyExpression, raw);
		return dec.ok;
	}

private:
	CryptoConfig config_;
};

} // namespace

bool SgxTeeBridge::isCompiledIn()
{
#ifdef ABE_SPARK_SGX_ENABLED
	return true;
#else
	return false;
#endif
}

bool SgxTeeBridge::isAvailable(const CryptoConfig& config)
{
	if (config.tee_mode != "sgx") return false;
	if (!isCompiledIn()) return false;
	return instance().ensureLoaded(config);
}

SgxTeeBridge& SgxTeeBridge::instance()
{
	static SgxTeeBridge s;
	return s;
}

SgxTeeBridge::SgxTeeBridge()
	: loaded_(false)
	, eid_(0)
{
}

SgxTeeBridge::~SgxTeeBridge()
{
	destroy();
}

bool SgxTeeBridge::ensureLoaded(const CryptoConfig& config)
{
	std::lock_guard<std::mutex> lock(mu_);
	if (loaded_) return true;
	if (config.sgx_enclave_path.empty()) return false;
	return loadEnclave(config.sgx_enclave_path);
}

void SgxTeeBridge::destroy()
{
	std::lock_guard<std::mutex> lock(mu_);
#ifdef ABE_SPARK_SGX_ENABLED
	if (loaded_ && eid_ != 0) {
		sgx_destroy_enclave(static_cast<sgx_enclave_id_t>(eid_));
		eid_ = 0;
	}
#endif
	loaded_ = false;
	enclave_path_.clear();
}

bool SgxTeeBridge::loadEnclave(const std::string& path)
{
#ifdef ABE_SPARK_SGX_ENABLED
	sgx_launch_token_t token;
	std::memset(&token, 0, sizeof(token));
	int updated = 0;
	sgx_enclave_id_t eid = 0;
	sgx_status_t ret = sgx_create_enclave(
		path.c_str(), SGX_DEBUG_FLAG, &token, &updated, &eid, NULL);
	if (ret != SGX_SUCCESS) return false;
	eid_ = static_cast<uint64_t>(eid);
	enclave_path_ = path;
	loaded_ = true;
	return true;
#else
	(void)path;
	return false;
#endif
}

bool SgxTeeBridge::initAbe()
{
#ifdef ABE_SPARK_SGX_ENABLED
	if (!loaded_) return false;
	int ok = 0;
	sgx_status_t callRet = SGX_SUCCESS;
	sgx_status_t ret = ecall_spark_abe_init(
		static_cast<sgx_enclave_id_t>(eid_), &callRet, &ok);
	return ret == SGX_SUCCESS && callRet == SGX_SUCCESS && ok == 1;
#else
	return false;
#endif
}

bool SgxTeeBridge::issueUser(const std::string& userId, const abe::AttributeSet& attrs)
{
#ifdef ABE_SPARK_SGX_ENABLED
	if (!loaded_) return false;
	int ok = 0;
	const std::string csv = attrsToCsv(attrs);
	sgx_status_t callRet = SGX_SUCCESS;
	sgx_status_t ret = ecall_spark_abe_issue_user(
		static_cast<sgx_enclave_id_t>(eid_), &callRet,
		userId.c_str(), csv.c_str(), &ok);
	return ret == SGX_SUCCESS && callRet == SGX_SUCCESS && ok == 1;
#else
	(void)userId;
	(void)attrs;
	return false;
#endif
}

DekEncryptResult SgxTeeBridge::encryptDek(
	const std::string& policy,
	const std::vector<uint8_t>& dek)
{
	DekEncryptResult out;
	out.ok = false;
#ifdef ABE_SPARK_SGX_ENABLED
	if (!loaded_) {
		out.reason = "SGX enclave not loaded";
		return out;
	}
	std::vector<uint8_t> ct(65536);
	size_t ctLen = 0;
	int ok = 0;
	sgx_status_t callRet = SGX_SUCCESS;
	sgx_status_t ret = ecall_spark_abe_encrypt_dek(
		static_cast<sgx_enclave_id_t>(eid_), &callRet,
		policy.c_str(),
		dek.empty() ? NULL : &dek[0], dek.size(),
		ct.empty() ? NULL : &ct[0], ct.size(),
		&ctLen, &ok);
	if (ret != SGX_SUCCESS || callRet != SGX_SUCCESS || ok != 1) {
		out.reason = "ecall_spark_abe_encrypt_dek failed";
		return out;
	}
	ct.resize(ctLen);
	out.encrypted_dek_base64 = bytesToBase64(ct);
	out.ok = true;
#else
	(void)policy;
	(void)dek;
	out.reason = "SGX not compiled in";
#endif
	return out;
}

DekDecryptResult SgxTeeBridge::decryptDek(
	const std::string& userId,
	const std::string& policy,
	const std::vector<uint8_t>& ctRaw)
{
	DekDecryptResult out;
	out.ok = false;
#ifdef ABE_SPARK_SGX_ENABLED
	if (!loaded_) {
		out.reason = "SGX enclave not loaded";
		return out;
	}
	std::vector<uint8_t> dek(256);
	size_t dekLen = 0;
	int ok = 0;
	sgx_status_t callRet = SGX_SUCCESS;
	sgx_status_t ret = ecall_spark_abe_decrypt_dek(
		static_cast<sgx_enclave_id_t>(eid_), &callRet,
		userId.c_str(), policy.c_str(),
		ctRaw.empty() ? NULL : &ctRaw[0], ctRaw.size(),
		dek.empty() ? NULL : &dek[0], dek.size(),
		&dekLen, &ok);
	if (ret != SGX_SUCCESS || callRet != SGX_SUCCESS || ok != 1) {
		out.reason = "ecall_spark_abe_decrypt_dek failed";
		return out;
	}
	dek.resize(dekLen);
	out.dek = dek;
	out.ok = true;
#else
	(void)userId;
	(void)policy;
	(void)ctRaw;
	out.reason = "SGX not compiled in";
#endif
	return out;
}

std::shared_ptr<ICPABECrypto> createCpabeCrypto(const CryptoConfig& config)
{
	if (config.tee_mode == "sgx" && SgxTeeBridge::isCompiledIn()) {
		try {
			return std::shared_ptr<ICPABECrypto>(new SgxTeeCpabeCrypto(config));
		} catch (...) {
			// Enclave 不可用时回退进程内 FAME（tee.mode=sim 等价）
		}
	}
	return std::shared_ptr<ICPABECrypto>(new InProcessCpabeCrypto(config));
}

} // namespace abe_spark
