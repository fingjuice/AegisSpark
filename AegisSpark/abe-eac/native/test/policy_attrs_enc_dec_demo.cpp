/**
 * 本地 TEE 模拟：策略属性个数（10,20,...,100）vs CP-ABE 加密/解密耗时。
 *
 * 路径与系统一致：CryptoProvider → encryptDek / decryptDek（abe_spark_core）。
 * 策略：attr:0 and attr:1 and ... and attr:N-1（N 个叶子属性）。
 * 解密用户持有全部 N 个属性，保证可解密。
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
#include <string>
#include <vector>

namespace {

/** N 个属性的 AND 策略：attr:0 and attr:1 and ... and attr:(N-1) */
std::string makeAndPolicy(size_t nAttrs)
{
	if (nAttrs < 1) nAttrs = 1;
	std::ostringstream oss;
	for (size_t i = 0; i < nAttrs; ++i) {
		if (i > 0) oss << " and ";
		oss << "attr:" << i;
	}
	return oss.str();
}

abe::AttributeSet makeAttrs(size_t nAttrs)
{
	abe::AttributeSet attrs;
	for (size_t i = 0; i < nAttrs; ++i) {
		std::ostringstream oss;
		oss << "attr:" << i;
		attrs.insert(oss.str());
	}
	return attrs;
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

} // namespace

int main(int argc, char** argv)
{
	const char* outPath = (argc > 1) ? argv[1]
		: "/home/shanlicheng/test-benchmark/ABE-Spark/result/policy-attrs/enc_dec_vs_attrs.csv";
	int reps = (argc > 2) ? std::atoi(argv[2]) : 40;
	if (reps < 1) reps = 40;

	const std::string cfgPath = abe_spark::IniConfigLoader::resolveConfigPath();
	abe_spark::CryptoConfig cfg = abe_spark::CryptoConfigLoader::loadFromFile(cfgPath);
	abe_spark::CryptoProvider provider(cfg);
	const abe::PublicParams& pp = provider.kgc()->publicParams();
	const std::vector<uint8_t> dek(16, 0x5A);

	printf("=== Policy attrs vs encrypt/decrypt (TEE-sim: abe_spark_core) ===\n");
	printf("config=%s curve=%s engine=%s reps=%d\n",
		cfgPath.c_str(), cfg.curve_name.c_str(), cfg.abe_engine.c_str(), reps);
	printf("policy=AND of N attrs (attr:0 and ... and attr:N-1)\n");

	// 全局预热
	{
		std::string warmPol = makeAndPolicy(50);
		abe::AttributeSet warmAttrs = makeAttrs(50);
		abe::UserSecretKey warmUsk =
			provider.kgc()->issueUserAttributes("warmup", warmAttrs);
		for (int i = 0; i < 30; ++i) {
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
		}
	}

	std::ofstream ofs(outPath);
	if (!ofs) {
		fprintf(stderr, "cannot open %s\n", outPath);
		return 1;
	}
	ofs << "num_attrs,policy_bytes,encrypt_avg_ms,decrypt_avg_ms,"
	    << "encrypt_total_ms,decrypt_total_ms,encrypted_dek_b64_len,reps,timestamp\n";

	// 正反序各半，减轻顺序偏差
	std::vector<size_t> sizes;
	for (size_t n = 100; n >= 10; n -= 10) sizes.push_back(n);
	for (size_t n = 10; n <= 100; n += 10) sizes.push_back(n);
	const int half = (reps + 1) / 2;

	struct Acc {
		double encMs;
		double decMs;
		size_t polBytes;
		size_t encBlob;
		int repsDone;
		Acc() : encMs(0), decMs(0), polBytes(0), encBlob(0), repsDone(0) {}
	};
	std::map<size_t, Acc> acc;

	for (size_t si = 0; si < sizes.size(); ++si) {
		size_t nAttrs = sizes[si];
		std::string policy = makeAndPolicy(nAttrs);
		size_t leaves = countLeaves(policy);
		if (leaves != nAttrs) {
			fprintf(stderr, "leaf count mismatch want=%zu got=%zu policy=%s\n",
				nAttrs, leaves, policy.c_str());
			return 1;
		}

		abe::AttributeSet attrs = makeAttrs(nAttrs);
		abe::UserSecretKey usk =
			provider.kgc()->issueUserAttributes("user_n" + std::to_string(nAttrs), attrs);

		for (int i = 0; i < half; ++i) {
			auto t0 = std::chrono::steady_clock::now();
			abe_spark::DekEncryptResult er = provider.cpabe()->encryptDek(pp, dek, policy);
			auto t1 = std::chrono::steady_clock::now();
			if (!er.ok) {
				fprintf(stderr, "encrypt failed n=%zu: %s\n", nAttrs, er.reason.c_str());
				return 1;
			}

			auto t2 = std::chrono::steady_clock::now();
			abe_spark::DekDecryptResult dr = provider.cpabe()->decryptDek(
				pp, usk, er.encrypted_dek_base64, policy);
			auto t3 = std::chrono::steady_clock::now();
			if (!dr.ok) {
				fprintf(stderr, "decrypt failed n=%zu: %s\n", nAttrs, dr.reason.c_str());
				return 1;
			}
			if (dr.dek != dek) {
				fprintf(stderr, "dek mismatch n=%zu\n", nAttrs);
				return 1;
			}

			Acc& a = acc[nAttrs];
			a.encMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
			a.decMs += std::chrono::duration<double, std::milli>(t3 - t2).count();
			a.polBytes = policy.size();
			a.encBlob = er.encrypted_dek_base64.size();
			a.repsDone += 1;
		}
	}

	std::time_t now = std::time(nullptr);
	char ts[64];
	std::strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S", std::localtime(&now));

	for (size_t n = 10; n <= 100; n += 10) {
		Acc& a = acc[n];
		double encAvg = a.repsDone > 0 ? a.encMs / a.repsDone : 0.0;
		double decAvg = a.repsDone > 0 ? a.decMs / a.repsDone : 0.0;
		ofs << n << "," << a.polBytes << ","
		    << encAvg << "," << decAvg << ","
		    << a.encMs << "," << a.decMs << ","
		    << a.encBlob << "," << a.repsDone << "," << ts << "\n";
		printf("attrs=%3zu pol_bytes=%4zu enc_avg=%.3f ms  dec_avg=%.3f ms  enc_b64=%zu\n",
			n, a.polBytes, encAvg, decAvg, a.encBlob);
	}

	printf("wrote %s\n", outPath);
	return 0;
}
