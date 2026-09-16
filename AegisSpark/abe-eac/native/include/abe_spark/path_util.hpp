#pragma once

#include <string>
#include <vector>

namespace abe_spark {

/** HDFS URI 路径规范化与授权前缀匹配 */
class PathUtil {
public:
	static std::string normalize(const std::string& hdfsUri);

	/** target 是否严格位于 root 目录之下（含 root 本身） */
	static bool isUnderRoot(const std::string& target, const std::string& root);

	/** target 是否位于任一白名单根目录下 */
	static bool isUnderAnyRoot(const std::string& target, const std::vector<std::string>& roots);

	/** ticket 绑定路径与写入目标是否一致（规范化后前缀匹配） */
	static bool ticketBindsTarget(const std::string& ticketPath, const std::string& writeTarget);
};

} // namespace abe_spark
