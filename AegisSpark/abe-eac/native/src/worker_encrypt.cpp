#include "abe_spark/worker_encrypt.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>

#include <openssl/rand.h>

namespace abe_spark {
namespace {

static const char* kColOrder[] = {
	"employee_id", "name", "email", "phone", "salary", "department", "address", "bank_account"
};
static const size_t kColCount = 8;
static const size_t kColWidth[] = {16, 48, 64, 16, 12, 32, 96, 24};

void padField(char* dest, size_t width, const std::string& s)
{
	memset(dest, 0, width);
	memcpy(dest, s.c_str(), std::min(s.size(), width));
}

std::vector<uint8_t> randomDek(size_t len, std::mt19937& rng)
{
	std::vector<uint8_t> dek(len);
	if (RAND_bytes(dek.data(), static_cast<int>(len)) != 1) {
		for (size_t i = 0; i < len; ++i) {
			dek[i] = static_cast<uint8_t>(rng());
		}
	}
	return dek;
}

std::vector<uint8_t> encryptColumnSegment(
	IAESGCMCrypto& aes,
	const std::vector<uint8_t>& dek,
	const uint8_t* plain, size_t len,
	int nonce_bytes, int tag_bytes, uint8_t nonce_byte)
{
	std::vector<uint8_t> nonce(static_cast<size_t>(nonce_bytes), nonce_byte);
	std::vector<uint8_t> pt(plain, plain + len);
	AesGcmResult enc = aes.encrypt(dek, nonce, pt);
	if (!enc.ok) return std::vector<uint8_t>();
	std::vector<uint8_t> out;
	out.insert(out.end(), nonce.begin(), nonce.end());
	out.insert(out.end(), enc.output.begin(), enc.output.end());
	return out;
}

} // namespace

CompanyEncryptResult encryptCompanyTable(
	CryptoProvider& provider,
	uint64_t company_id,
	uint32_t record_count,
	const std::string& policy_public,
	const std::string& policy_sensitive,
	const std::string& hdfs_file_path_prefix,
	SharedDekStore* shared)
{
	CompanyEncryptResult out;
	out.ok = false;
	out.abe_encrypt_ms = 0.0;
	out.abe_encrypt_calls = 0;
	out.aes_encrypt_ms = 0.0;

	static const char* first[] = {"James", "Mary", "Wei", "Fang", "John", "Li", "Chen", "Yuki"};
	static const char* last[] = {"Smith", "Wang", "Zhang", "Liu", "Brown", "Zhao", "Yang", "Huang"};
	static const char* depts[] = {"Engineering", "Finance", "HR", "Sales", "Marketing", "Operations", "Legal", "IT"};
	static const char* domains[] = {"acmecorp.com", "globex.io", "initech.com", "umbrella.co"};

	std::mt19937 rng(static_cast<unsigned>(42 + company_id * 7919));
	const uint32_t nrec = record_count > 0 ? record_count : static_cast<uint32_t>(50 + (rng() % 251));

	char tname[32];
	snprintf(tname, sizeof(tname), "company_%06llu", (unsigned long long)company_id);
	out.table_name = tname;
	out.record_count = nrec;

	std::vector<ColumnByteRange> ranges;
	size_t total = 0;
	for (size_t c = 0; c < kColCount; ++c) total += kColWidth[c] * nrec;
	std::vector<uint8_t> plaintext(total, 0);

	size_t offset = 0;
	for (size_t c = 0; c < kColCount; ++c) {
		ColumnByteRange r;
		r.column_name = kColOrder[c];
		r.byte_offset = static_cast<int64_t>(offset);
		r.byte_length = static_cast<int64_t>(kColWidth[c] * nrec);
		ranges.push_back(r);

		const bool sensitive = (r.column_name == "email" || r.column_name == "phone" ||
			r.column_name == "salary" || r.column_name == "bank_account");

		for (uint32_t i = 0; i < nrec; ++i) {
			std::string val;
			const std::string fn = first[rng() % 8];
			const std::string ln = last[rng() % 8];
			if (r.column_name == std::string("employee_id")) {
				char buf[32];
				snprintf(buf, sizeof(buf), "EMP%07llu%04u", (unsigned long long)company_id, i);
				val = buf;
			} else if (r.column_name == std::string("name")) val = fn + " " + ln;
			else if (r.column_name == std::string("email")) val = fn + "." + ln + "@" + domains[rng() % 4];
			else if (r.column_name == std::string("phone")) {
				char buf[16];
				snprintf(buf, sizeof(buf), "1%010lu", (unsigned long)(rng() % 1000000000U));
				val = buf;
			} else if (r.column_name == std::string("salary")) {
				val = std::to_string(8000 + rng() % 77000);
			} else if (r.column_name == std::string("department")) val = depts[rng() % 8];
			else if (r.column_name == std::string("address")) {
				val = std::to_string(1 + rng() % 999) + " Enterprise Ave, Shanghai, CN";
			} else if (r.column_name == std::string("bank_account")) {
				char buf[24];
				snprintf(buf, sizeof(buf), "6222%015lu", (unsigned long)(rng() % 1000000000U));
				val = buf;
			}
			(void)sensitive;
			padField(reinterpret_cast<char*>(&plaintext[offset + i * kColWidth[c]]),
				kColWidth[c], val);
		}
		offset += kColWidth[c] * nrec;
	}

	out.plain_bytes = plaintext.size();
	out.header.file_path = hdfs_file_path_prefix + out.table_name + ".enc";

	const CryptoConfig& cfg = provider.config();
	std::vector<std::vector<uint8_t> > deks(2);
	struct Group { std::string policy; std::vector<size_t> cols; bool sensitive; };
	Group groups[2];
	groups[0].policy = policy_public;
	groups[0].sensitive = false;
	groups[1].policy = policy_sensitive;
	groups[1].sensitive = true;
	for (size_t c = 0; c < kColCount; ++c) {
		const bool sens = (std::string(kColOrder[c]) == "email" || std::string(kColOrder[c]) == "phone" ||
			std::string(kColOrder[c]) == "salary" || std::string(kColOrder[c]) == "bank_account");
		groups[sens ? 1 : 0].cols.push_back(c);
	}

	for (int g = 0; g < 2; ++g) {
		if (groups[g].cols.empty()) continue;
		HsecColumn ah;
		ah.policy_expression = groups[g].policy;
		for (size_t i = 0; i < groups[g].cols.size(); ++i) {
			ah.column_scope.push_back(kColOrder[groups[g].cols[i]]);
		}

		bool reused = false;
		if (shared != nullptr) {
			std::lock_guard<std::mutex> lock(shared->mu);
			std::map<std::string, std::pair<std::vector<uint8_t>, std::string> >::iterator it =
				shared->by_policy.find(groups[g].policy);
			if (it != shared->by_policy.end()) {
				deks[g] = it->second.first;
				ah.encrypted_dek = it->second.second;
				reused = true;
			}
		}
		if (!reused) {
			deks[g] = randomDek(16, rng);
			const auto tAbe0 = std::chrono::steady_clock::now();
			DekEncryptResult er = provider.cpabe()->encryptDek(
				provider.kgc()->publicParams(), deks[g], groups[g].policy);
			const auto tAbe1 = std::chrono::steady_clock::now();
			out.abe_encrypt_ms += std::chrono::duration<double, std::milli>(tAbe1 - tAbe0).count();
			out.abe_encrypt_calls += 1;
			if (!er.ok) {
				out.reason = "encryptDek failed";
				return out;
			}
			ah.encrypted_dek = er.encrypted_dek_base64;
			if (shared != nullptr) {
				std::lock_guard<std::mutex> lock(shared->mu);
				shared->by_policy[groups[g].policy] =
					std::make_pair(deks[g], ah.encrypted_dek);
			}
		}
		out.header.abe_headers.push_back(ah);
	}

	const auto tAes0 = std::chrono::steady_clock::now();
	std::vector<uint8_t> ciphertext;
	for (size_t c = 0; c < kColCount; ++c) {
		const bool sens = (std::string(kColOrder[c]) == "email" || std::string(kColOrder[c]) == "phone" ||
			std::string(kColOrder[c]) == "salary" || std::string(kColOrder[c]) == "bank_account");
		const int g = sens ? 1 : 0;
		const size_t off = static_cast<size_t>(ranges[c].byte_offset);
		const size_t len = static_cast<size_t>(ranges[c].byte_length);
		std::vector<uint8_t> seg = encryptColumnSegment(
			*provider.aesGcm(), deks[g], &plaintext[off], len,
			cfg.aead_nonce_bytes, cfg.aead_tag_bytes, static_cast<uint8_t>(c + 1));
		if (seg.empty()) {
			out.reason = "aes encrypt failed";
			return out;
		}
		ciphertext.insert(ciphertext.end(), seg.begin(), seg.end());
	}
	const auto tAes1 = std::chrono::steady_clock::now();
	out.aes_encrypt_ms = std::chrono::duration<double, std::milli>(tAes1 - tAes0).count();

	out.ciphertext = ciphertext;
	out.ranges = ranges;
	out.enc_bytes = ciphertext.size();
	out.ok = true;
	return out;
}

} // namespace abe_spark
