#include "abe_spark/crypto_config.hpp"
#include "abe_spark/ini_config.hpp"

#include <cstdlib>
#include <stdexcept>

namespace abe_spark {
namespace {

std::string firstKey(const std::map<std::string, std::string>& kv,
	const std::string& primary, const std::string& legacy)
{
	if (kv.count(primary) && !kv.at(primary).empty()) return kv.at(primary);
	if (kv.count(legacy) && !kv.at(legacy).empty()) return kv.at(legacy);
	return "";
}

} // namespace

const mcl::CurveParam* CryptoConfig::curveParam() const
{
	if (curve_name == "BLS12_381") return &mcl::BLS12_381;
	if (curve_name == "BN254") return &mcl::BN254;
	if (curve_name == "BN381") return &mcl::BN381_2;
	if (curve_name == "SNARK1") return &mcl::BN_SNARK1;
	throw std::invalid_argument("unsupported curve_name: " + curve_name);
}

abe::SetupConfig CryptoConfig::toAbeSetupConfig() const
{
	abe::SetupConfig cfg;
	cfg.curveParam = curveParam();
	return cfg;
}

std::string CryptoConfigLoader::expandEnv(const std::string& value)
{
	return IniConfigLoader::expandEnv(value);
}

int CryptoConfigLoader::parseInt(const std::string& key, const std::string& value, int defaultValue)
{
	return IniConfigLoader::parseInt(key, value, defaultValue);
}

CryptoConfig CryptoConfigLoader::loadFromMap(const std::map<std::string, std::string>& kv)
{
	CryptoConfig cfg;
	cfg.curve_name = kv.count("curve.name") ? kv.at("curve.name") : "BLS12_381";
	cfg.fp_bit = parseInt("curve.fp_bit", kv.count("curve.fp_bit") ? kv.at("curve.fp_bit") : "384", 384);
	cfg.fr_bit = parseInt("curve.fr_bit", kv.count("curve.fr_bit") ? kv.at("curve.fr_bit") : "256", 256);
	cfg.abe_engine = kv.count("abe.engine") ? kv.at("abe.engine") : "fame";
	cfg.scheme_id = kv.count("abe.scheme_id") ? kv.at("abe.scheme_id") : "ABE-Spark/FAME-AC17-v1";
	cfg.tee_mode = kv.count("tee.mode") ? kv.at("tee.mode") : "sim";
	if (const char* envTee = std::getenv("ABE_TEE_MODE")) {
		if (envTee[0] != '\0') cfg.tee_mode = envTee;
	}
	cfg.sgx_enclave_path = expandEnv(kv.count("tee.enclave_path") ? kv.at("tee.enclave_path") : "");
	cfg.aead_algorithm = kv.count("aead.algorithm") ? kv.at("aead.algorithm") : "AES-128-GCM";
	cfg.aead_key_bits = parseInt("aead.key_bits", kv.count("aead.key_bits") ? kv.at("aead.key_bits") : "128", 128);
	cfg.aead_nonce_bytes = parseInt("aead.nonce_bytes", kv.count("aead.nonce_bytes") ? kv.at("aead.nonce_bytes") : "12", 12);
	cfg.aead_tag_bytes = parseInt("aead.tag_bytes", kv.count("aead.tag_bytes") ? kv.at("aead.tag_bytes") : "16", 16);
	cfg.signature_algorithm = kv.count("signature.algorithm") ? kv.at("signature.algorithm") : "ECDSA-P256-SHA256";
	cfg.admin_private_key_path = expandEnv(kv.count("signature.admin_private_key_path") ? kv.at("signature.admin_private_key_path") : "");
	cfg.admin_public_key_path = expandEnv(kv.count("signature.admin_public_key_path") ? kv.at("signature.admin_public_key_path") : "");
	cfg.driver_private_key_path = expandEnv(kv.count("signature.driver_private_key_path") ? kv.at("signature.driver_private_key_path") : "");
	cfg.driver_public_key_path = expandEnv(kv.count("signature.driver_public_key_path") ? kv.at("signature.driver_public_key_path") : "");
	cfg.write_verification_private_key_path = expandEnv(
		firstKey(kv, "signature.write_verification_private_key_path",
			"signature.eac_manager_private_key_path"));
	cfg.write_verification_public_key_path = expandEnv(
		firstKey(kv, "signature.write_verification_public_key_path",
			"signature.eac_manager_public_key_path"));
	return cfg;
}

CryptoConfig CryptoConfigLoader::loadFromFile(const std::string& path)
{
	return loadFromMap(IniConfigLoader::loadMapFromFile(path));
}

} // namespace abe_spark
