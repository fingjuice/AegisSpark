#include "abe_spark/runtime.hpp"
#include "abe_spark/types_json.hpp"
#include "abe_spark/worker_encrypt.hpp"

#include <jni.h>

#include <string>
#include <vector>

namespace {

jbyteArray toJByteArray(JNIEnv* env, const std::vector<uint8_t>& data)
{
	jbyteArray arr = env->NewByteArray(static_cast<jsize>(data.size()));
	if (!arr) return NULL;
	if (!data.empty()) {
		env->SetByteArrayRegion(arr, 0, static_cast<jsize>(data.size()),
			reinterpret_cast<const jbyte*>(&data[0]));
	}
	return arr;
}

std::vector<uint8_t> fromJByteArray(JNIEnv* env, jbyteArray arr)
{
	std::vector<uint8_t> out;
	if (!arr) return out;
	jsize len = env->GetArrayLength(arr);
	out.resize(static_cast<size_t>(len));
	env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(&out[0]));
	return out;
}

std::string jstringToStd(JNIEnv* env, jstring s)
{
	if (!s) return std::string();
	const char* chars = env->GetStringUTFChars(s, NULL);
	if (!chars) return std::string();
	std::string out(chars);
	env->ReleaseStringUTFChars(s, chars);
	return out;
}

abe::AttributeSet attrsFromJava(JNIEnv* env, jobjectArray attrArray)
{
	abe::AttributeSet attrs;
	if (!attrArray) return attrs;
	jsize n = env->GetArrayLength(attrArray);
	for (jsize i = 0; i < n; ++i) {
		jstring js = (jstring)env->GetObjectArrayElement(attrArray, i);
		attrs.insert(jstringToStd(env, js));
		env->DeleteLocalRef(js);
	}
	return attrs;
}

/** 最近一次 processWorkerRead 的 CP-ABE / AES 计时（线程本地） */
thread_local double g_last_abe_decrypt_ms = 0.0;
thread_local double g_last_abe_decrypt_calls = 0.0;
thread_local double g_last_aes_decrypt_ms = 0.0;
thread_local double g_last_abe_cache_hits = 0.0;

} // namespace

extern "C" {

JNIEXPORT void JNICALL Java_org_apache_spark_abe_AbeNativeBridge_setDekCacheEnabled(
	JNIEnv*, jclass, jboolean enabled)
{
	abe_spark::AbeRuntime::instance().setDekCacheEnabled(enabled == JNI_TRUE);
}

JNIEXPORT void JNICALL Java_org_apache_spark_abe_AbeNativeBridge_clearDekCache(
	JNIEnv*, jclass)
{
	abe_spark::AbeRuntime::instance().clearDekCache();
}

JNIEXPORT void JNICALL Java_org_apache_spark_abe_AbeNativeBridge_setSharedDekEnabled(
	JNIEnv*, jclass, jboolean enabled)
{
	abe_spark::AbeRuntime::instance().setSharedDekEnabled(enabled == JNI_TRUE);
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_abe_AbeNativeBridge_init(
	JNIEnv* env, jclass, jstring configPath)
{
	return abe_spark::AbeRuntime::instance().init(jstringToStd(env, configPath)) ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_org_apache_spark_abe_AbeNativeBridge_setUserContext(
	JNIEnv* env, jclass, jstring userId, jobjectArray attributes)
{
	abe_spark::AbeRuntime::instance().setUserContext(
		jstringToStd(env, userId), attrsFromJava(env, attributes));
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_abe_AbeNativeBridge_isNativeLoaded(
	JNIEnv*, jclass)
{
	return JNI_TRUE;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_abe_AbeNativeBridge_processWorkerRead(
	JNIEnv* env, jclass,
	jbyteArray encryptedStream,
	jstring headerJson,
	jstring columnRangesJson)
{
	g_last_abe_decrypt_ms = 0.0;
	g_last_abe_decrypt_calls = 0.0;
	g_last_aes_decrypt_ms = 0.0;
	g_last_abe_cache_hits = 0.0;
	try {
		abe_spark::Hsec header = abe_spark::TypesJson::hsecFromJson(
			jstringToStd(env, headerJson));
		std::vector<abe_spark::ColumnByteRange> ranges =
			abe_spark::TypesJson::columnByteRangesFromJson(jstringToStd(env, columnRangesJson));
		abe_spark::WorkerReadResult result = abe_spark::AbeRuntime::instance().processWorkerRead(
			fromJByteArray(env, encryptedStream), header, ranges);
		g_last_abe_decrypt_ms = result.abe_decrypt_ms;
		g_last_abe_decrypt_calls = static_cast<double>(result.abe_decrypt_calls);
		g_last_aes_decrypt_ms = result.aes_decrypt_ms;
		g_last_abe_cache_hits = static_cast<double>(result.abe_cache_hits);
		if (!result.ok) return NULL;
		return toJByteArray(env, result.plaintext);
	} catch (...) {
		return NULL;
	}
}

/**
 * 返回最近一次 processWorkerRead 计时：
 * [0]=abe_decrypt_ms_total, [1]=abe_decrypt_calls(miss), [2]=aes_decrypt_ms, [3]=cache_hits
 */
JNIEXPORT jdoubleArray JNICALL Java_org_apache_spark_abe_AbeNativeBridge_lastWorkerReadTiming(
	JNIEnv* env, jclass)
{
	jdoubleArray arr = env->NewDoubleArray(4);
	if (!arr) return NULL;
	jdouble vals[4] = {
		g_last_abe_decrypt_ms,
		g_last_abe_decrypt_calls,
		g_last_aes_decrypt_ms,
		g_last_abe_cache_hits
	};
	env->SetDoubleArrayRegion(arr, 0, 4, vals);
	return arr;
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_abe_AbeNativeBridge_verifyEd25519(
	JNIEnv* env, jclass,
	jbyteArray message,
	jstring signatureHex,
	jstring publicKeyPemPath)
{
	try {
		if (!abe_spark::AbeRuntime::instance().isInitialized()) return JNI_FALSE;
		abe_spark::CryptoProvider* p = abe_spark::AbeRuntime::instance().provider();
		if (!p) return JNI_FALSE;
		abe_spark::VerifyResult vr = p->ecdsa()->verify(
			fromJByteArray(env, message),
			jstringToStd(env, signatureHex),
			jstringToStd(env, publicKeyPemPath));
		return vr.ok ? JNI_TRUE : JNI_FALSE;
	} catch (...) {
		return JNI_FALSE;
	}
}

JNIEXPORT jstring JNICALL Java_org_apache_spark_abe_AbeNativeBridge_encryptCompanyTable(
	JNIEnv* env, jclass,
	jlong companyId,
	jint recordCount,
	jstring policyPublic,
	jstring policySensitive,
	jstring hdfsPathPrefix)
{
	try {
		std::string json = abe_spark::AbeRuntime::instance().encryptCompanyTableJson(
			static_cast<uint64_t>(companyId),
			static_cast<uint32_t>(recordCount),
			jstringToStd(env, policyPublic),
			jstringToStd(env, policySensitive),
			jstringToStd(env, hdfsPathPrefix));
		return env->NewStringUTF(json.c_str());
	} catch (...) {
		return env->NewStringUTF("{\"ok\":false,\"reason\":\"native exception\"}");
	}
}

JNIEXPORT jstring JNICALL Java_org_apache_spark_abe_AbeNativeBridge_signEd25519(
	JNIEnv* env, jclass,
	jbyteArray message,
	jstring privateKeyPemPath)
{
	try {
		if (!abe_spark::AbeRuntime::instance().isInitialized()) return NULL;
		abe_spark::CryptoProvider* p = abe_spark::AbeRuntime::instance().provider();
		if (!p) return NULL;
		abe_spark::SignResult sr = p->ecdsa()->sign(
			fromJByteArray(env, message), jstringToStd(env, privateKeyPemPath));
		if (!sr.ok) return NULL;
		return env->NewStringUTF(sr.signature_hex.c_str());
	} catch (...) {
		return NULL;
	}
}

} // extern "C"
