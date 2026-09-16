#include "abe_spark/types_json.hpp"

#include <cctype>
#include <sstream>
#include <utility>

namespace abe_spark {
namespace {

std::string escapeJsonString(const std::string& s)
{
	std::string out;
	out.reserve(s.size() + 8);
	for (size_t i = 0; i < s.size(); ++i) {
		const char c = s[i];
		switch (c) {
		case '"': out += "\\\""; break;
		case '\\': out += "\\\\"; break;
		case '\b': out += "\\b"; break;
		case '\f': out += "\\f"; break;
		case '\n': out += "\\n"; break;
		case '\r': out += "\\r"; break;
		case '\t': out += "\\t"; break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[7];
				snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
				out += buf;
			} else {
				out.push_back(c);
			}
		}
	}
	return out;
}

void skipWs(const std::string& json, size_t& pos)
{
	while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) {
		++pos;
	}
}

bool expectChar(const std::string& json, size_t& pos, char ch)
{
	skipWs(json, pos);
	if (pos >= json.size() || json[pos] != ch) return false;
	++pos;
	return true;
}

std::string parseJsonString(const std::string& json, size_t& pos)
{
	skipWs(json, pos);
	if (pos >= json.size() || json[pos] != '"') {
		throw JsonParseError("expected JSON string");
	}
	++pos;
	std::string out;
	while (pos < json.size()) {
		char c = json[pos++];
		if (c == '"') return out;
		if (c == '\\') {
			if (pos >= json.size()) throw JsonParseError("unterminated escape");
			char e = json[pos++];
			switch (e) {
			case '"': out.push_back('"'); break;
			case '\\': out.push_back('\\'); break;
			case '/': out.push_back('/'); break;
			case 'b': out.push_back('\b'); break;
			case 'f': out.push_back('\f'); break;
			case 'n': out.push_back('\n'); break;
			case 'r': out.push_back('\r'); break;
			case 't': out.push_back('\t'); break;
			case 'u':
				if (pos + 4 > json.size()) throw JsonParseError("invalid unicode escape");
				out.push_back('?');
				pos += 4;
				break;
			default: throw JsonParseError("invalid escape sequence");
			}
		} else {
			out.push_back(c);
		}
	}
	throw JsonParseError("unterminated JSON string");
}

int64_t parseJsonInt64(const std::string& json, size_t& pos)
{
	skipWs(json, pos);
	size_t start = pos;
	if (pos < json.size() && (json[pos] == '-' || json[pos] == '+')) ++pos;
	while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) ++pos;
	if (start == pos) throw JsonParseError("expected JSON number");
	return strtoll(json.substr(start, pos - start).c_str(), NULL, 10);
}

void parseJsonStringArray(const std::string& json, size_t& pos, std::vector<std::string>& out)
{
	if (!expectChar(json, pos, '[')) throw JsonParseError("expected JSON array");
	skipWs(json, pos);
	if (pos < json.size() && json[pos] == ']') {
		++pos;
		return;
	}
	for (;;) {
		out.push_back(parseJsonString(json, pos));
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
		if (pos < json.size() && json[pos] == ']') {
			++pos;
			return;
		}
		throw JsonParseError("expected ',' or ']' in array");
	}
}

std::string jsonStringArray(const std::vector<std::string>& values)
{
	std::ostringstream oss;
	oss << "[";
	for (size_t i = 0; i < values.size(); ++i) {
		if (i > 0) oss << ",";
		oss << "\"" << escapeJsonString(values[i]) << "\"";
	}
	oss << "]";
	return oss.str();
}

} // namespace

std::string TypesJson::toJson(const AdminEndorsement& v)
{
	std::ostringstream oss;
	oss << "{"
		<< "\"app_code_hash\":\"" << escapeJsonString(v.app_code_hash) << "\","
		<< "\"allowed_root_directories\":" << jsonStringArray(v.allowed_root_directories) << ","
		<< "\"timestamp\":" << v.timestamp << ","
		<< "\"admin_signature\":\"" << escapeJsonString(v.admin_signature) << "\""
		<< "}";
	return oss.str();
}

AdminEndorsement TypesJson::adminEndorsementFromJson(const std::string& json)
{
	size_t pos = 0;
	AdminEndorsement out;
	if (!expectChar(json, pos, '{')) throw JsonParseError("expected object");
	for (;;) {
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == '}') {
			++pos;
			return out;
		}
		std::string key = parseJsonString(json, pos);
		if (!expectChar(json, pos, ':')) throw JsonParseError("expected ':'");
		if (key == "app_code_hash") {
			out.app_code_hash = parseJsonString(json, pos);
		} else if (key == "allowed_root_directories") {
			parseJsonStringArray(json, pos, out.allowed_root_directories);
		} else if (key == "timestamp") {
			out.timestamp = parseJsonInt64(json, pos);
		} else if (key == "admin_signature") {
			out.admin_signature = parseJsonString(json, pos);
		} else {
			throw JsonParseError("unknown field in AdminEndorsement: " + key);
		}
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
	}
}

std::string TypesJson::toJson(const HsecColumn& v)
{
	std::ostringstream oss;
	oss << "{"
		<< "\"column_scope\":" << jsonStringArray(v.column_scope) << ","
		<< "\"policy_expression\":\"" << escapeJsonString(v.policy_expression) << "\","
		<< "\"encrypted_dek\":\"" << escapeJsonString(v.encrypted_dek) << "\""
		<< "}";
	return oss.str();
}

HsecColumn TypesJson::hsecColumnFromJson(const std::string& json)
{
	size_t pos = 0;
	HsecColumn out;
	if (!expectChar(json, pos, '{')) throw JsonParseError("expected object");
	for (;;) {
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == '}') {
			++pos;
			return out;
		}
		std::string key = parseJsonString(json, pos);
		if (!expectChar(json, pos, ':')) throw JsonParseError("expected ':'");
		if (key == "column_scope") {
			parseJsonStringArray(json, pos, out.column_scope);
		} else if (key == "policy_expression") {
			out.policy_expression = parseJsonString(json, pos);
		} else if (key == "encrypted_dek") {
			out.encrypted_dek = parseJsonString(json, pos);
		} else {
			throw JsonParseError("unknown field in HsecColumn: " + key);
		}
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
	}
}

std::string TypesJson::toJson(const Hsec& v)
{
	std::ostringstream oss;
	oss << "{"
		<< "\"file_path\":\"" << escapeJsonString(v.file_path) << "\","
		<< "\"abe_headers\":[";
	for (size_t i = 0; i < v.abe_headers.size(); ++i) {
		if (i > 0) oss << ",";
		oss << toJson(v.abe_headers[i]);
	}
	oss << "]}";
	return oss.str();
}

Hsec TypesJson::hsecFromJson(const std::string& json)
{
	size_t pos = 0;
	Hsec out;
	if (!expectChar(json, pos, '{')) throw JsonParseError("expected object");
	for (;;) {
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == '}') {
			++pos;
			return out;
		}
		std::string key = parseJsonString(json, pos);
		if (!expectChar(json, pos, ':')) throw JsonParseError("expected ':'");
		if (key == "file_path") {
			out.file_path = parseJsonString(json, pos);
		} else if (key == "abe_headers") {
			if (!expectChar(json, pos, '[')) throw JsonParseError("expected array");
			skipWs(json, pos);
			if (pos < json.size() && json[pos] == ']') {
				++pos;
			} else {
				for (;;) {
					size_t itemStart = pos;
					int depth = 0;
					do {
						if (pos >= json.size()) throw JsonParseError("unterminated abe_headers item");
						if (json[pos] == '{') ++depth;
						if (json[pos] == '}') --depth;
						++pos;
					} while (depth > 0);
					out.abe_headers.push_back(hsecColumnFromJson(json.substr(itemStart, pos - itemStart)));
					skipWs(json, pos);
					if (pos < json.size() && json[pos] == ',') {
						++pos;
						continue;
					}
					if (pos < json.size() && json[pos] == ']') {
						++pos;
						break;
					}
					throw JsonParseError("expected ',' or ']' in abe_headers");
				}
			}
		} else {
			throw JsonParseError("unknown field in Hsec: " + key);
		}
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
	}
}

std::string TypesJson::toJson(const TaskTicket& v)
{
	std::ostringstream oss;
	oss << "{"
		<< "\"job_id\":\"" << escapeJsonString(v.job_id) << "\","
		<< "\"task_id\":\"" << escapeJsonString(v.task_id) << "\","
		<< "\"worker_ip\":\"" << escapeJsonString(v.worker_ip) << "\","
		<< "\"allowed_target_path\":\"" << escapeJsonString(v.allowed_target_path) << "\","
		<< "\"expires_at\":" << v.expires_at << ","
		<< "\"driver_signature\":\"" << escapeJsonString(v.driver_signature) << "\""
		<< "}";
	return oss.str();
}

TaskTicket TypesJson::taskTicketFromJson(const std::string& json)
{
	size_t pos = 0;
	TaskTicket out;
	if (!expectChar(json, pos, '{')) throw JsonParseError("expected object");
	for (;;) {
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == '}') {
			++pos;
			return out;
		}
		std::string key = parseJsonString(json, pos);
		if (!expectChar(json, pos, ':')) throw JsonParseError("expected ':'");
		if (key == "job_id") {
			out.job_id = parseJsonString(json, pos);
		} else if (key == "task_id") {
			out.task_id = parseJsonString(json, pos);
		} else if (key == "worker_ip") {
			out.worker_ip = parseJsonString(json, pos);
		} else if (key == "allowed_target_path") {
			out.allowed_target_path = parseJsonString(json, pos);
		} else if (key == "expires_at") {
			out.expires_at = parseJsonInt64(json, pos);
		} else if (key == "driver_signature") {
			out.driver_signature = parseJsonString(json, pos);
		} else {
			throw JsonParseError("unknown field in TaskTicket: " + key);
		}
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
	}
}

std::string TypesJson::toJson(const ColumnByteRange& v)
{
	std::ostringstream oss;
	oss << "{"
		<< "\"column_name\":\"" << escapeJsonString(v.column_name) << "\","
		<< "\"byte_offset\":" << v.byte_offset << ","
		<< "\"byte_length\":" << v.byte_length
		<< "}";
	return oss.str();
}

ColumnByteRange TypesJson::columnByteRangeFromJson(const std::string& json)
{
	size_t pos = 0;
	ColumnByteRange out;
	if (!expectChar(json, pos, '{')) throw JsonParseError("expected object");
	for (;;) {
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == '}') {
			++pos;
			return out;
		}
		std::string key = parseJsonString(json, pos);
		if (!expectChar(json, pos, ':')) throw JsonParseError("expected ':'");
		if (key == "column_name") {
			out.column_name = parseJsonString(json, pos);
		} else if (key == "byte_offset") {
			out.byte_offset = static_cast<uint64_t>(parseJsonInt64(json, pos));
		} else if (key == "byte_length") {
			out.byte_length = static_cast<uint64_t>(parseJsonInt64(json, pos));
		} else {
			throw JsonParseError("unknown field in ColumnByteRange: " + key);
		}
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
	}
}

std::string TypesJson::toJsonArray(const std::vector<ColumnByteRange>& ranges)
{
	std::ostringstream oss;
	oss << "[";
	for (size_t i = 0; i < ranges.size(); ++i) {
		if (i > 0) oss << ",";
		oss << toJson(ranges[i]);
	}
	oss << "]";
	return oss.str();
}

std::vector<ColumnByteRange> TypesJson::columnByteRangesFromJson(const std::string& json)
{
	size_t pos = 0;
	std::vector<ColumnByteRange> out;
	if (!expectChar(json, pos, '[')) throw JsonParseError("expected array");
	skipWs(json, pos);
	if (pos < json.size() && json[pos] == ']') {
		++pos;
		return out;
	}
	for (;;) {
		size_t itemStart = pos;
		int depth = 0;
		do {
			if (pos >= json.size()) throw JsonParseError("unterminated column range");
			if (json[pos] == '{') ++depth;
			if (json[pos] == '}') --depth;
			++pos;
		} while (depth > 0);
		out.push_back(columnByteRangeFromJson(json.substr(itemStart, pos - itemStart)));
		skipWs(json, pos);
		if (pos < json.size() && json[pos] == ',') {
			++pos;
			continue;
		}
		if (pos < json.size() && json[pos] == ']') {
			++pos;
			return out;
		}
		throw JsonParseError("expected ',' or ']' in column ranges");
	}
}

} // namespace abe_spark
