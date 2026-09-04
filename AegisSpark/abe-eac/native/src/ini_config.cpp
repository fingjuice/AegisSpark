#include "abe_spark/ini_config.hpp"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <stdexcept>

namespace abe_spark {
namespace {

std::string trim(const std::string& s)
{
	size_t b = 0;
	while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
	size_t e = s.size();
	while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
	return s.substr(b, e - b);
}

std::string stripInlineComment(const std::string& line)
{
	bool inQuote = false;
	for (size_t i = 0; i < line.size(); ++i) {
		if (line[i] == '"') inQuote = !inQuote;
		if (!inQuote && line[i] == '#') return line.substr(0, i);
	}
	return line;
}

} // namespace

void IniConfigLoader::ensureDefaultEnvVars()
{
	if (std::getenv("ABE_SPARK_ROOT") == NULL) {
		setenv("ABE_SPARK_ROOT", "/home/shanlicheng/test-benchmark/ABE-Spark", 0);
	}
	if (std::getenv("MCL_ROOT") == NULL) {
		setenv("MCL_ROOT", "/home/shanlicheng/mcl", 0);
	}
	if (std::getenv("ABE_SPARK_CONF_DIR") == NULL) {
		std::string confDir = getenvOr("ABE_SPARK_ROOT", "/home/shanlicheng/test-benchmark/ABE-Spark");
		confDir += "/abe-eac/conf/keys";
		setenv("ABE_SPARK_CONF_DIR", confDir.c_str(), 0);
	}
}

std::string IniConfigLoader::getenvOr(const std::string& name, const std::string& defaultValue)
{
	const char* v = std::getenv(name.c_str());
	return v != NULL ? std::string(v) : defaultValue;
}

std::string IniConfigLoader::expandEnv(const std::string& value)
{
	std::string out;
	for (size_t i = 0; i < value.size();) {
		if (value[i] == '$' && i + 1 < value.size() && value[i + 1] == '{') {
			size_t end = value.find('}', i + 2);
			if (end == std::string::npos) {
				out.push_back(value[i++]);
				continue;
			}
			std::string var = value.substr(i + 2, end - i - 2);
			const char* envVal = std::getenv(var.c_str());
			if (envVal != NULL) out += envVal;
			i = end + 1;
		} else {
			out.push_back(value[i++]);
		}
	}
	return out;
}

int IniConfigLoader::parseInt(const std::string& key, const std::string& value, int defaultValue)
{
	if (value.empty()) return defaultValue;
	char* end = NULL;
	long v = strtol(value.c_str(), &end, 10);
	if (end == value.c_str() || *end != '\0') {
		throw std::invalid_argument("invalid integer for config key: " + key);
	}
	return static_cast<int>(v);
}

double IniConfigLoader::parseDouble(const std::string& key, const std::string& value, double defaultValue)
{
	if (value.empty()) return defaultValue;
	char* end = NULL;
	double v = strtod(value.c_str(), &end);
	if (end == value.c_str() || *end != '\0') {
		throw std::invalid_argument("invalid double for config key: " + key);
	}
	return v;
}

bool IniConfigLoader::parseBool(const std::string& value, bool defaultValue)
{
	if (value.empty()) return defaultValue;
	std::string v = value;
	for (size_t i = 0; i < v.size(); ++i) {
		v[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(v[i])));
	}
	if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
	if (v == "0" || v == "false" || v == "no" || v == "off") return false;
	return defaultValue;
}

std::vector<std::string> IniConfigLoader::splitList(const std::string& value)
{
	std::vector<std::string> out;
	std::string item;
	for (size_t i = 0; i <= value.size(); ++i) {
		if (i == value.size() || value[i] == ',') {
			item = trim(item);
			if (!item.empty()) out.push_back(item);
			item.clear();
		} else {
			item.push_back(value[i]);
		}
	}
	return out;
}

std::map<std::string, std::string> IniConfigLoader::loadMapFromFile(const std::string& path)
{
	ensureDefaultEnvVars();
	std::ifstream in(path.c_str());
	if (!in) {
		throw std::runtime_error("failed to open config: " + path);
	}

	std::map<std::string, std::string> kv;
	std::string section;
	std::string line;
	while (std::getline(in, line)) {
		line = trim(stripInlineComment(line));
		if (line.empty()) continue;
		if (line.front() == '[' && line.back() == ']') {
			section = line.substr(1, line.size() - 2);
			continue;
		}
		size_t eq = line.find('=');
		if (eq == std::string::npos) continue;
		std::string key = trim(line.substr(0, eq));
		std::string val = expandEnv(trim(line.substr(eq + 1)));
		if (!section.empty()) key = section + "." + key;
		kv[key] = val;
	}
	return kv;
}

std::string IniConfigLoader::resolveConfigPath()
{
	ensureDefaultEnvVars();
	const char* env = std::getenv("ABE_SPARK_CONFIG");
	if (env != NULL && env[0] != '\0') return std::string(env);
	std::string root = getenvOr("ABE_SPARK_ROOT", "/home/shanlicheng/test-benchmark/ABE-Spark");
	return root + "/abe-eac/conf/abe-spark.conf";
}

} // namespace abe_spark
