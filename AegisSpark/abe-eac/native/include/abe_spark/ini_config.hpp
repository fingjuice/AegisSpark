#pragma once

#include <map>
#include <string>
#include <vector>

namespace abe_spark {

/** 简易 INI 解析，支持 ${ENV_VAR} 展开 */
class IniConfigLoader {
public:
	static void ensureDefaultEnvVars();
	static std::string getenvOr(const std::string& name, const std::string& defaultValue);
	static std::string expandEnv(const std::string& value);
	static std::map<std::string, std::string> loadMapFromFile(const std::string& path);
	static std::string resolveConfigPath();

	static int parseInt(const std::string& key, const std::string& value, int defaultValue);
	static double parseDouble(const std::string& key, const std::string& value, double defaultValue);
	static bool parseBool(const std::string& value, bool defaultValue);
	static std::vector<std::string> splitList(const std::string& value);
};

} // namespace abe_spark
