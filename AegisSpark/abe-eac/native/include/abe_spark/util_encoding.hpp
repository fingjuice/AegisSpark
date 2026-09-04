#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace abe_spark {

std::string bytesToHex(const std::vector<uint8_t>& bytes);
std::vector<uint8_t> hexToBytes(const std::string& hex);

std::string bytesToBase64(const std::vector<uint8_t>& bytes);
std::vector<uint8_t> base64ToBytes(const std::string& b64);

} // namespace abe_spark
