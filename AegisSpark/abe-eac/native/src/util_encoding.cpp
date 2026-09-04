#include "abe_spark/util_encoding.hpp"

#include <cctype>
#include <stdexcept>

namespace abe_spark {
namespace {

int hexValue(char c)
{
	if (c >= '0' && c <= '9') return c - '0';
	if (c >= 'a' && c <= 'f') return c - 'a' + 10;
	if (c >= 'A' && c <= 'F') return c - 'A' + 10;
	return -1;
}

static const char kBase64Alphabet[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

} // namespace

std::string bytesToHex(const std::vector<uint8_t>& bytes)
{
	static const char* hex = "0123456789abcdef";
	std::string out;
	out.reserve(bytes.size() * 2);
	for (size_t i = 0; i < bytes.size(); ++i) {
		out.push_back(hex[(bytes[i] >> 4) & 0x0f]);
		out.push_back(hex[bytes[i] & 0x0f]);
	}
	return out;
}

std::vector<uint8_t> hexToBytes(const std::string& hex)
{
	if (hex.size() % 2 != 0) {
		throw std::invalid_argument("hex string length must be even");
	}
	std::vector<uint8_t> out;
	out.reserve(hex.size() / 2);
	for (size_t i = 0; i < hex.size(); i += 2) {
		int hi = hexValue(hex[i]);
		int lo = hexValue(hex[i + 1]);
		if (hi < 0 || lo < 0) {
			throw std::invalid_argument("invalid hex character");
		}
		out.push_back(static_cast<uint8_t>((hi << 4) | lo));
	}
	return out;
}

std::string bytesToBase64(const std::vector<uint8_t>& bytes)
{
	std::string out;
	out.reserve(((bytes.size() + 2) / 3) * 4);
	size_t i = 0;
	while (i + 2 < bytes.size()) {
		uint32_t n = (static_cast<uint32_t>(bytes[i]) << 16) |
			(static_cast<uint32_t>(bytes[i + 1]) << 8) |
			static_cast<uint32_t>(bytes[i + 2]);
		out.push_back(kBase64Alphabet[(n >> 18) & 63]);
		out.push_back(kBase64Alphabet[(n >> 12) & 63]);
		out.push_back(kBase64Alphabet[(n >> 6) & 63]);
		out.push_back(kBase64Alphabet[n & 63]);
		i += 3;
	}
	if (i < bytes.size()) {
		uint32_t n = static_cast<uint32_t>(bytes[i]) << 16;
		out.push_back(kBase64Alphabet[(n >> 18) & 63]);
		if (i + 1 < bytes.size()) {
			n |= static_cast<uint32_t>(bytes[i + 1]) << 8;
			out.push_back(kBase64Alphabet[(n >> 12) & 63]);
			out.push_back(kBase64Alphabet[(n >> 6) & 63]);
			out.push_back('=');
		} else {
			out.push_back(kBase64Alphabet[(n >> 12) & 63]);
			out.push_back('=');
			out.push_back('=');
		}
	}
	return out;
}

std::vector<uint8_t> base64ToBytes(const std::string& b64)
{
	auto decodeChar = [](char c) -> int {
		if (c >= 'A' && c <= 'Z') return c - 'A';
		if (c >= 'a' && c <= 'z') return c - 'a' + 26;
		if (c >= '0' && c <= '9') return c - '0' + 52;
		if (c == '+') return 62;
		if (c == '/') return 63;
		if (c == '=') return -2;
		return -1;
	};

	std::vector<uint8_t> out;
	int val = 0;
	int valb = -8;
	for (size_t i = 0; i < b64.size(); ++i) {
		int c = decodeChar(b64[i]);
		if (c == -1) continue;
		if (c == -2) break;
		val = (val << 6) + c;
		valb += 6;
		if (valb >= 0) {
			out.push_back(static_cast<uint8_t>((val >> valb) & 0xff));
			valb -= 8;
		}
	}
	return out;
}

} // namespace abe_spark
