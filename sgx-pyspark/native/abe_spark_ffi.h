#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** 初始化运行时（加载 sgx-pyspark.conf） */
int abe_spark_init(const char* config_path);

/** 设置 Worker 用户属性上下文 */
int abe_spark_set_user_context(const char* user_id, const char* attributes_json);

/** 释放 FFI 返回的堆内存 */
void abe_spark_free(void* ptr);

/** 获取最近一次错误的描述 */
const char* abe_spark_last_error(void);

/** 为用户签发 ABE 属性私钥，返回 JSON */
char* abe_spark_issue_user_attributes(const char* user_id, const char* attributes_json);

/** 管理员签署 Spark 作业，返回 AdminEndorsement JSON */
char* abe_spark_endorse_spark_job(
	const char* binary_path,
	const char* allowed_dirs_json,
	int64_t timestamp);

/** Driver TEE 远程认证握手，返回 JSON */
char* abe_spark_driver_attestation(const char* eac_url, const char* endorsement_json);

/** Driver TEE 派生 Task Ticket，返回 JSON */
char* abe_spark_derive_task_ticket(
	const char* job_id,
	const char* task_id,
	const char* worker_ip,
	const char* allowed_target_path,
	int64_t expires_at);

/** Write Verification 信任链验证，返回 JSON */
char* abe_spark_verify_write_chain(
	const char* ticket_json,
	const char* endorsement_json,
	const char* target_path,
	int64_t now_epoch_sec);

/** EAC 验证 + 单调计数器 + 代理写，返回 JSON（密文写入 output_path） */
char* abe_spark_proxy_write(
	const char* ticket_json,
	const char* endorsement_json,
	const char* target_path,
	const unsigned char* encrypted_data,
	size_t encrypted_len,
	int64_t now_epoch_sec,
	const char* output_path);

/** Worker TEE 读管道：ABE 解 DEK + AES-GCM 解密/脱敏，返回 base64 明文 */
char* abe_spark_process_worker_read(
	const unsigned char* encrypted_stream,
	size_t encrypted_len,
	const char* header_json,
	const char* column_ranges_json);

/** 加密演示表（生成密文 + header + layout），返回 JSON */
char* abe_spark_encrypt_demo_table(
	const char* policy_public,
	const char* policy_sensitive,
	const char* output_prefix);

#ifdef __cplusplus
}
#endif
