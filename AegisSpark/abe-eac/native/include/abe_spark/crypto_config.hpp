#pragma once

#include <abe_framework.hpp>

#include <cstdint>
#include <map>
#include <string>

namespace abe_spark {

/** 从 abe-spark.conf（或兼容的 INI）加载密码学运行时配置 */
struct CryptoConfig {
	std::string curve_name;
	int fp_bit;
	int fr_bit;

	std::string abe_engine;
	std::string scheme_id;

	/** TEE 模式：sim（进程内）| sgx（Intel SGX Enclave） */
	std::string tee_mode;
	std::string sgx_enclave_path;

	std::string aead_algorithm;
	int aead_key_bits;
	int aead_nonce_bytes;
	int aead_tag_bytes;

	std::string signature_algorithm;
	std::string admin_private_key_path;
	std::string admin_public_key_path;
	std::string driver_private_key_path;
	std::string driver_public_key_path;
	std::string write_verification_private_key_path;
	std::string write_verification_public_key_path;

	/** 解析曲线名为 mcl CurveParam 指针；未知曲线抛异常 */
	const mcl::CurveParam* curveParam() const;

	/** 构造 abe::SetupConfig */
	abe::SetupConfig toAbeSetupConfig() const;
};

/** 简易 INI 配置加载器，支持 ${ENV_VAR} 环境变量展开 */
class CryptoConfigLoader {
public:
	static CryptoConfig loadFromFile(const std::string& path);
	static CryptoConfig loadFromMap(const std::map<std::string, std::string>& kv);

private:
	static std::string expandEnv(const std::string& value);
	static int parseInt(const std::string& key, const std::string& value, int defaultValue);
};

} // namespace abe_spark
