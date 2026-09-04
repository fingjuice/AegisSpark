#include "abe_spark/path_util.hpp"

#include <algorithm>
#include <cctype>

namespace abe_spark {
namespace {

std::string stripTrailingSlash(const std::string& s)
{
	if (s.size() > 1 && s.back() == '/') {
		return s.substr(0, s.size() - 1);
	}
	return s;
}

} // namespace

std::string PathUtil::normalize(const std::string& hdfsUri)
{
	std::string out = hdfsUri;
	while (!out.empty() && std::isspace(static_cast<unsigned char>(out.front()))) {
		out.erase(out.begin());
	}
	while (!out.empty() && std::isspace(static_cast<unsigned char>(out.back()))) {
		out.pop_back();
	}
	if (out.empty()) return out;
	return stripTrailingSlash(out);
}

bool PathUtil::isUnderRoot(const std::string& target, const std::string& root)
{
	const std::string t = normalize(target);
	const std::string r = normalize(root);
	if (r.empty() || t.empty()) return false;
	if (t == r) return true;
	if (t.size() <= r.size()) return false;
	if (t.compare(0, r.size(), r) != 0) return false;
	return t[r.size()] == '/';
}

bool PathUtil::isUnderAnyRoot(const std::string& target, const std::vector<std::string>& roots)
{
	for (size_t i = 0; i < roots.size(); ++i) {
		if (isUnderRoot(target, roots[i])) return true;
	}
	return false;
}

bool PathUtil::ticketBindsTarget(const std::string& ticketPath, const std::string& writeTarget)
{
	const std::string t = normalize(ticketPath);
	const std::string w = normalize(writeTarget);
	if (t.empty() || w.empty()) return false;
	return t == w || isUnderRoot(w, t);
}

} // namespace abe_spark
