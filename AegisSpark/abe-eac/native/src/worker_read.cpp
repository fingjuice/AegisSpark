#include "abe_spark/worker_read.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <map>
#include <unordered_map>

namespace abe_spark {
namespace {

static const char kNullMask[] = "NULL";

bool findRangeForColumn(
	const std::string& column,
	const std::vector<ColumnByteRange>& ranges,
	ColumnByteRange& out)
{
	for (size_t i = 0; i < ranges.size(); ++i) {
		if (ranges[i].column_name == column) {
			out = ranges[i];
			return true;
		}
	}
	return false;
}

size_t computePlaintextSize(const std::vector<ColumnAccessPlan>& plan)
{
	size_t maxEnd = 0;
	for (size_t i = 0; i < plan.size(); ++i) {
		const size_t end = static_cast<size_t>(plan[i].range.byte_offset + plan[i].range.byte_length);
		if (end > maxEnd) maxEnd = end;
	}
	return maxEnd;
}

struct PlanSortByOffset {
	bool operator()(const ColumnAccessPlan& a, const ColumnAccessPlan& b) const
	{
		return a.range.byte_offset < b.range.byte_offset;
	}
};

} // namespace

void WorkerReadPipeline::fillNullMask(uint8_t* dest, size_t len)
{
	static const size_t kNullLen = 4;
	for (size_t i = 0; i < len; ++i) {
		dest[i] = static_cast<uint8_t>(kNullMask[i % kNullLen]);
	}
}

std::vector<ColumnAccessPlan> WorkerReadPipeline::buildAccessPlan(
	const Hsec& header,
	const std::vector<ColumnByteRange>& column_ranges,
	ICPABECrypto& cpabe,
	const abe::PublicParams& pp,
	const abe::UserSecretKey& usk,
	size_t* abe_decrypt_calls,
	double* abe_decrypt_ms,
	std::unordered_map<std::string, std::vector<uint8_t> >* dek_cache,
	size_t* abe_cache_hits)
{
	std::map<std::string, ColumnAccessPlan> planByColumn;
	size_t calls = 0;
	size_t hits = 0;
	double abeMs = 0.0;

	for (size_t h = 0; h < header.abe_headers.size(); ++h) {
		const HsecColumn& abeHeader = header.abe_headers[h];
		const std::string cacheKey = abeHeader.encrypted_dek + "|" + abeHeader.policy_expression;

		DekDecryptResult dekResult;
		dekResult.ok = false;
		bool cacheHit = false;
		if (dek_cache != nullptr) {
			std::unordered_map<std::string, std::vector<uint8_t> >::iterator it =
				dek_cache->find(cacheKey);
			if (it != dek_cache->end()) {
				dekResult.ok = true;
				dekResult.dek = it->second;
				cacheHit = true;
				++hits;
			}
		}
		if (!cacheHit) {
			const auto t0 = std::chrono::steady_clock::now();
			dekResult = cpabe.decryptDek(
				pp, usk, abeHeader.encrypted_dek, abeHeader.policy_expression);
			const auto t1 = std::chrono::steady_clock::now();
			++calls;
			abeMs += std::chrono::duration<double, std::milli>(t1 - t0).count();
			if (dek_cache != nullptr && dekResult.ok) {
				(*dek_cache)[cacheKey] = dekResult.dek;
			}
		}

		for (size_t c = 0; c < abeHeader.column_scope.size(); ++c) {
			const std::string& colName = abeHeader.column_scope[c];
			ColumnByteRange range;
			if (!findRangeForColumn(colName, column_ranges, range)) {
				continue;
			}

			ColumnAccessPlan plan;
			plan.range = range;
			plan.policy_expression = abeHeader.policy_expression;

			if (dekResult.ok && !planByColumn.count(colName)) {
				plan.authorized = true;
				plan.dek = dekResult.dek;
			} else if (!dekResult.ok && !planByColumn.count(colName)) {
				plan.authorized = false;
				plan.deny_reason = dekResult.reason.empty()
					? "CP-ABE pairing failed"
					: dekResult.reason;
			} else {
				continue;
			}
			planByColumn[colName] = plan;
		}
	}

	// 未出现在 ABE 头中的列默认拒绝
	for (size_t i = 0; i < column_ranges.size(); ++i) {
		const std::string& colName = column_ranges[i].column_name;
		if (planByColumn.find(colName) == planByColumn.end()) {
			ColumnAccessPlan plan;
			plan.range = column_ranges[i];
			plan.authorized = false;
			plan.deny_reason = "no abe header for column";
			planByColumn[colName] = plan;
		}
	}

	if (abe_decrypt_calls) *abe_decrypt_calls = calls;
	if (abe_decrypt_ms) *abe_decrypt_ms = abeMs;
	if (abe_cache_hits) *abe_cache_hits = hits;

	std::vector<ColumnAccessPlan> out;
	for (std::map<std::string, ColumnAccessPlan>::const_iterator it = planByColumn.begin();
		 it != planByColumn.end(); ++it) {
		out.push_back(it->second);
	}
	std::sort(out.begin(), out.end(), PlanSortByOffset());
	return out;
}

WorkerReadResult WorkerReadPipeline::processStream(
	const std::vector<uint8_t>& encrypted_stream,
	const std::vector<ColumnAccessPlan>& plan,
	IAESGCMCrypto& aes_gcm,
	int nonce_bytes,
	int tag_bytes)
{
	WorkerReadResult result;
	result.columns_authorized = 0;
	result.columns_masked = 0;
	result.abe_decrypt_calls = 0;
	result.abe_decrypt_ms = 0.0;
	result.abe_cache_hits = 0;
	result.aes_decrypt_ms = 0.0;
	if (plan.empty()) {
		result.ok = false;
		result.reason = "empty access plan";
		return result;
	}

	const size_t outSize = computePlaintextSize(plan);
	result.plaintext.assign(outSize, 0);

	size_t cursor = 0;
	for (size_t i = 0; i < plan.size(); ++i) {
		const ColumnAccessPlan& col = plan[i];
		const size_t segNonce = static_cast<size_t>(nonce_bytes);
		const size_t segCt = static_cast<size_t>(col.range.byte_length);
		const size_t segTag = static_cast<size_t>(tag_bytes);
		const size_t segTotal = segNonce + segCt + segTag;

		if (cursor + segTotal > encrypted_stream.size()) {
			result.ok = false;
			result.reason = "encrypted stream truncated at column " + col.range.column_name;
			return result;
		}

		const uint8_t* segStart = &encrypted_stream[cursor];
		std::vector<uint8_t> nonce(segStart, segStart + segNonce);
		std::vector<uint8_t> ciphertext_with_tag(
			segStart + segNonce, segStart + segTotal);
		cursor += segTotal;

		uint8_t* dest = &result.plaintext[col.range.byte_offset];
		const size_t destLen = static_cast<size_t>(col.range.byte_length);

		if (col.authorized && col.dek.size() >= 16) {
			const auto t0 = std::chrono::steady_clock::now();
			AesGcmResult dec = aes_gcm.decrypt(col.dek, nonce, ciphertext_with_tag);
			const auto t1 = std::chrono::steady_clock::now();
			result.aes_decrypt_ms +=
				std::chrono::duration<double, std::milli>(t1 - t0).count();
			if (dec.ok && dec.output.size() == destLen) {
				std::memcpy(dest, dec.output.data(), destLen);
				++result.columns_authorized;
			} else {
				fillNullMask(dest, destLen);
				++result.columns_masked;
			}
		} else {
			fillNullMask(dest, destLen);
			++result.columns_masked;
		}
	}

	result.ok = true;
	return result;
}

} // namespace abe_spark
