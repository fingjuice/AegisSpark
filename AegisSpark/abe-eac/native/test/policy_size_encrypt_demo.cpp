/**
 * 本地 TEE 模拟：FAME (AC17) CP-ABE，策略字节长度 10~100 vs 加密/解密耗时。
 *
 * - 引擎：fame（Agrawal–Chase FAME @ BLS12-381）
 * - 明文：256-bit（32 字节）对称密钥 DEK
 * - 策略：精确 N **字节**的真实风格 DNF 策略
 *   例：(role:analyst and dept:finance) or (role:admin and clearance:3) or ...
 * - 解密用户持有策略全部叶子属性
 *
 * 路径：CryptoProvider → encryptDek / decryptDek（abe_spark_core）。
 */
#include "abe_spark/crypto_config.hpp"
#include "abe_spark/crypto_iface.hpp"
#include "abe_spark/ini_config.hpp"

#include <abe_framework.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

const char* kRoles[] = {
	"analyst", "auditor", "admin", "manager", "engineer", "scientist", "operator"
};
const char* kDepts[] = {
	"finance", "hr", "legal", "security", "research", "ops", "sales"
};
const char* kProjects[] = {
	"alpha", "beta", "gamma", "delta", "omega", "nova", "orbit"
};
const size_t kNumRoles = sizeof(kRoles) / sizeof(kRoles[0]);
const size_t kNumDepts = sizeof(kDepts) / sizeof(kDepts[0]);
const size_t kNumProjects = sizeof(kProjects) / sizeof(kProjects[0]);

/** 真实风格子句：(role:X and dept:Y) 或 (role:X and clearance:N) 或 (role:X and project:P) */
std::string makeClause(size_t idx)
{
	const char* role = kRoles[idx % kNumRoles];
	std::ostringstream oss;
	switch (idx % 3) {
	case 0:
		oss << "(role:" << role << " and dept:" << kDepts[idx % kNumDepts] << ")";
		break;
	case 1:
		oss << "(role:" << role << " and clearance:" << (1 + static_cast<int>(idx % 5)) << ")";
		break;
	default:
		oss << "(role:" << role << " and project:" << kProjects[idx % kNumProjects] << ")";
		break;
	}
	return oss.str();
}

/** 将策略精确填到 nbytes：优先追加 or 子句，剩余字节并入末尾叶子（_id…）或短 tag 叶子。 */
std::string padToExactBytes(std::string pol, size_t nbytes)
{
	if (pol.size() > nbytes) {
		throw std::runtime_error("policy longer than target");
	}
	size_t idx = 1;
	while (true) {
		std::string add = " or " + makeClause(idx);
		if (pol.size() + add.size() > nbytes) break;
		pol += add;
		++idx;
	}
	if (pol.size() == nbytes) return pol;

	const size_t need = nbytes - pol.size();
	const std::string tagPrefix = " or tag:";
	if (need >= tagPrefix.size() + 1) {
		pol += tagPrefix;
		pol.append(nbytes - pol.size(), '0');
		return pol;
	}

	// 剩余极少字节：扩写最后一个属性名（去掉收尾 ')' 再还原）
	bool hadParen = false;
	if (!pol.empty() && pol.back() == ')') {
		pol.pop_back();
		hadParen = true;
	}
	if (pol.size() >= nbytes) {
		pol.resize(nbytes);
		return pol;
	}
	pol.push_back('_');
	while (pol.size() + (hadParen ? 1 : 0) < nbytes) pol.push_back('0');
	if (hadParen) {
		if (pol.size() + 1 > nbytes) pol.resize(nbytes - 1);
		pol.push_back(')');
	}
	while (pol.size() < nbytes) pol.push_back('0');
	if (pol.size() > nbytes) pol.resize(nbytes);
	return pol;
}

/**
 * 构造精确为 nbytes 的真实风格策略（自变量是字节长度，不是属性个数）。
 * 形态：DNF —— (role and dept|clearance|project) or (role and ...) or ...
 */
std::string makePolicy(size_t nbytes)
{
	if (nbytes < 6) nbytes = 6;

	std::string pol = makeClause(0);
	if (pol.size() == nbytes) return pol;
	if (pol.size() < nbytes) return padToExactBytes(pol, nbytes);

	// 首子句已超长：换用更短的真实属性表达式
	static const char* kShort[] = {
		"role:admin",                       // 10
		"dept:finance",                     // 12
		"role:analyst",                     // 12
		"(role:admin and dept:hr)",         // 23
		"(role:admin and clearance:3)",     // 28
		"(role:analyst and dept:finance)",  // 31
	};
	std::string best;
	for (size_t i = 0; i < sizeof(kShort) / sizeof(kShort[0]); ++i) {
		std::string s = kShort[i];
		if (s.size() == nbytes) return s;
		if (s.size() < nbytes && (best.empty() || s.size() > best.size())) best = s;
	}
	if (!best.empty()) return padToExactBytes(best, nbytes);

	pol = "role:";
	while (pol.size() < nbytes) pol.push_back('a');
	pol.resize(nbytes);
	return pol;
}

size_t countLeaves(const std::string& policy)
{
	size_t n = 0;
	for (size_t i = 0; i < policy.size(); ++i) {
		char c = policy[i];
		if (c == ' ' || c == '(' || c == ')') continue;
		std::string low;
		size_t j = i;
		while (j < policy.size() && policy[j] != ' ' && policy[j] != '(' && policy[j] != ')') {
			char ch = policy[j];
			if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
			low.push_back(ch);
			++j;
		}
		i = j - 1;
		if (low != "and" && low != "or") ++n;
	}
	return n;
}

abe::AttributeSet extractAttrs(const std::string& policy)
{
	abe::AttributeSet attrs;
	for (size_t i = 0; i < policy.size(); ++i) {
		char c = policy[i];
		if (c == ' ' || c == '(' || c == ')') continue;
		std::string tok;
		size_t j = i;
		while (j < policy.size() && policy[j] != ' ' && policy[j] != '(' && policy[j] != ')') {
			char ch = policy[j];
			if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
			tok.push_back(ch);
			++j;
		}
		i = j - 1;
		if (tok != "and" && tok != "or") attrs.insert(tok);
	}
	return attrs;
}

} // namespace

int main(int argc, char** argv)
{
	const char* outPath = (argc > 1) ? argv[1]
		: "/home/shanlicheng/test-benchmark/ABE-Spark/result/policy-size/encrypt_vs_policy.csv";
	int reps = (argc > 2) ? std::atoi(argv[2]) : 40;
	if (reps < 1) reps = 40;

	const std::string cfgPath = abe_spark::IniConfigLoader::resolveConfigPath();
	abe_spark::CryptoConfig cfg = abe_spark::CryptoConfigLoader::loadFromFile(cfgPath);
	cfg.abe_engine = "fame";
	cfg.scheme_id = "ABE-Spark/FAME-AC17-v1";
	abe_spark::CryptoProvider provider(cfg);
	const abe::PublicParams& pp = provider.kgc()->publicParams();

	// 256-bit DEK
	const std::vector<uint8_t> dek(32, 0x5A);

	printf("=== Policy-size FAME CP-ABE encrypt/decrypt (TEE-sim: abe_spark_core) ===\n");
	printf("config=%s curve=%s engine=%s dek_bits=%zu reps=%d\n",
		cfgPath.c_str(), cfg.curve_name.c_str(), cfg.abe_engine.c_str(),
		dek.size() * 8, reps);
	printf("scheme=%s current_engine=%s\n",
		pp.schemeId.c_str(), abe::ABEApi::currentEngine().c_str());

	// 全局预热
	{
		std::string warmPol = makePolicy(50);
		abe::AttributeSet warmAttrs = extractAttrs(warmPol);
		abe::UserSecretKey warmUsk =
			provider.kgc()->issueUserAttributes("warmup", warmAttrs);
		for (int i = 0; i < 20; ++i) {
			abe_spark::DekEncryptResult er = provider.cpabe()->encryptDek(pp, dek, warmPol);
			if (!er.ok) {
				fprintf(stderr, "warmup encrypt failed: %s\n", er.reason.c_str());
				return 1;
			}
			abe_spark::DekDecryptResult dr = provider.cpabe()->decryptDek(
				pp, warmUsk, er.encrypted_dek_base64, warmPol);
			if (!dr.ok) {
				fprintf(stderr, "warmup decrypt failed: %s\n", dr.reason.c_str());
				return 1;
			}
			if (dr.dek != dek) {
				fprintf(stderr, "warmup dek mismatch\n");
				return 1;
			}
		}
	}

	std::ofstream ofs(outPath);
	if (!ofs) {
		fprintf(stderr, "cannot open %s\n", outPath);
		return 1;
	}
	ofs << "policy_bytes,policy_leaves,encrypt_avg_ms,decrypt_avg_ms,"
	    << "encrypt_total_ms,decrypt_total_ms,encrypted_dek_b64_len,dek_bits,reps,"
	    << "engine,policy_sample,timestamp\n";

	std::vector<size_t> sizes;
	for (size_t n = 100; n >= 10; n -= 10) sizes.push_back(n);
	for (size_t n = 10; n <= 100; n += 10) sizes.push_back(n);
	const int half = (reps + 1) / 2;

	struct Acc {
		double encMs;
		double decMs;
		size_t leaves;
		size_t encBlob;
		std::string policy;
		int repsDone;
		Acc() : encMs(0), decMs(0), leaves(0), encBlob(0), repsDone(0) {}
	};
	std::map<size_t, Acc> acc;

	for (size_t si = 0; si < sizes.size(); ++si) {
		size_t nbytes = sizes[si];
		std::string policy = makePolicy(nbytes);
		if (policy.size() != nbytes) {
			fprintf(stderr, "policy length mismatch want=%zu got=%zu [%s]\n",
				nbytes, policy.size(), policy.c_str());
			return 1;
		}
		abe::AttributeSet attrs = extractAttrs(policy);
		abe::UserSecretKey usk = provider.kgc()->issueUserAttributes(
			"user_pol" + std::to_string(nbytes), attrs);

		for (int i = 0; i < half; ++i) {
			auto t0 = std::chrono::steady_clock::now();
			abe_spark::DekEncryptResult er = provider.cpabe()->encryptDek(pp, dek, policy);
			auto t1 = std::chrono::steady_clock::now();
			if (!er.ok) {
				fprintf(stderr, "encrypt failed nbytes=%zu: %s\n", nbytes, er.reason.c_str());
				return 1;
			}

			auto t2 = std::chrono::steady_clock::now();
			abe_spark::DekDecryptResult dr = provider.cpabe()->decryptDek(
				pp, usk, er.encrypted_dek_base64, policy);
			auto t3 = std::chrono::steady_clock::now();
			if (!dr.ok) {
				fprintf(stderr, "decrypt failed nbytes=%zu: %s\n", nbytes, dr.reason.c_str());
				return 1;
			}
			if (dr.dek != dek) {
				fprintf(stderr, "dek mismatch nbytes=%zu\n", nbytes);
				return 1;
			}

			Acc& a = acc[nbytes];
			a.encMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
			a.decMs += std::chrono::duration<double, std::milli>(t3 - t2).count();
			a.leaves = countLeaves(policy);
			a.encBlob = er.encrypted_dek_base64.size();
			a.policy = policy;
			a.repsDone += 1;
		}
	}

	std::time_t now = std::time(nullptr);
	char ts[64];
	std::strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", std::localtime(&now));

	for (size_t nbytes = 10; nbytes <= 100; nbytes += 10) {
		Acc& a = acc[nbytes];
		double encAvg = a.repsDone > 0 ? a.encMs / a.repsDone : 0.0;
		double decAvg = a.repsDone > 0 ? a.decMs / a.repsDone : 0.0;
		ofs << nbytes << "," << a.leaves << ","
		    << encAvg << "," << decAvg << ","
		    << a.encMs << "," << a.decMs << ","
		    << a.encBlob << "," << (dek.size() * 8) << "," << a.repsDone
		    << ",fame,\"" << a.policy << "\"," << ts << "\n";
		printf("policy_bytes=%3zu leaves=%2zu enc_avg=%.3f ms  dec_avg=%.3f ms  enc_b64=%zu\n  policy=%s\n",
			nbytes, a.leaves, encAvg, decAvg, a.encBlob, a.policy.c_str());
	}

	printf("wrote %s\n", outPath);
	return 0;
}
