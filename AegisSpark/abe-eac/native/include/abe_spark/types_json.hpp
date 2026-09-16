#pragma once

#include "abe_spark/types.hpp"

#include <stdexcept>
#include <string>

namespace abe_spark {

class JsonParseError : public std::runtime_error {
public:
	explicit JsonParseError(const std::string& msg)
		: std::runtime_error(msg)
	{
	}
};

/** JSON 序列化 / 反序列化（严格字段名，与路线文档 JSON schema 对齐） */
class TypesJson {
public:
	static std::string toJson(const AdminEndorsement& v);
	static AdminEndorsement adminEndorsementFromJson(const std::string& json);

	static std::string toJson(const HsecColumn& v);
	static HsecColumn hsecColumnFromJson(const std::string& json);

	static std::string toJson(const Hsec& v);
	static Hsec hsecFromJson(const std::string& json);

	static std::string toJson(const TaskTicket& v);
	static TaskTicket taskTicketFromJson(const std::string& json);

	static std::string toJson(const ColumnByteRange& v);
	static ColumnByteRange columnByteRangeFromJson(const std::string& json);
	static std::string toJsonArray(const std::vector<ColumnByteRange>& ranges);
	static std::vector<ColumnByteRange> columnByteRangesFromJson(const std::string& json);
};

} // namespace abe_spark
