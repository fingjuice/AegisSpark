cmake_minimum_required(VERSION 3.10)
project(abe_spark_native LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(MCL_ROOT "/home/shanlicheng/mcl" CACHE PATH "mcl library root")
set(MCL_BUILD_DIR "${MCL_ROOT}/build-eac" CACHE PATH "mcl build directory")
set(SGX_SDK "$ENV{SGX_SDK}" CACHE PATH "Intel SGX SDK root")
if(NOT SGX_SDK)
	set(SGX_SDK "/opt/intel/sgxsdk")
endif()
set(SGX_ENCLAVE_DIR "${MCL_ROOT}/sgx-enclave" CACHE PATH "SGX enclave project root")
set(SGX_STUB_DIR "${SGX_ENCLAVE_DIR}/build/generated/App" CACHE PATH "SGX Enclave_u stubs")
option(ABE_SPARK_ENABLE_SGX "Link Intel SGX URTS (tee.mode=sgx on driver)" ON)

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

set(ABE_SPARK_CORE_SRCS
	src/types.cpp
	src/types_json.cpp
	src/util_encoding.cpp
	src/crypto_config.cpp
	src/ini_config.cpp
	src/spark_config.cpp
	src/crypto_impl.cpp
	src/sgx_tee_bridge.cpp
	src/path_util.cpp
	src/worker_read.cpp
	src/worker_encrypt.cpp
	src/worker_write.cpp
	src/driver_tee.cpp
	src/write_verification_tee.cpp
	src/runtime.cpp
)

set(ABE_SPARK_SGX_ENABLED OFF)
if(ABE_SPARK_ENABLE_SGX AND EXISTS "${SGX_SDK}/include/sgx_urts.h" AND EXISTS "${SGX_STUB_DIR}/Enclave_u.c")
	set(ABE_SPARK_SGX_ENABLED ON)
	list(APPEND ABE_SPARK_CORE_SRCS "${SGX_STUB_DIR}/Enclave_u.c")
	message(STATUS "ABE-Spark SGX TEE: enabled (${SGX_SDK})")
else()
	message(STATUS "ABE-Spark SGX TEE: disabled (ABE_SPARK_ENABLE_SGX=${ABE_SPARK_ENABLE_SGX})")
endif()

add_library(abe_spark_core ${ABE_SPARK_CORE_SRCS})

target_include_directories(abe_spark_core
	PUBLIC
		${CMAKE_CURRENT_SOURCE_DIR}/include
		${MCL_ROOT}/ABE-Framework/include
		${MCL_ROOT}/include
		${MCL_ROOT}/src
)

if(ABE_SPARK_SGX_ENABLED)
	target_include_directories(abe_spark_core
		PUBLIC
			${SGX_SDK}/include
			${SGX_STUB_DIR}
	)
	target_compile_definitions(abe_spark_core PUBLIC ABE_SPARK_SGX_ENABLED=1)
	target_link_directories(abe_spark_core PUBLIC ${SGX_SDK}/lib64)
	set_source_files_properties("${SGX_STUB_DIR}/Enclave_u.c" PROPERTIES LANGUAGE C)
	set_target_properties(abe_spark_core PROPERTIES
		BUILD_RPATH "${SGX_SDK}/lib64"
		INSTALL_RPATH "${SGX_SDK}/lib64"
	)
	target_link_libraries(abe_spark_core PUBLIC sgx_urts_sim)
endif()

# 与 mcl build-eac 保持一致，避免头文件内联模板与 libmcl.a 符号冲突
target_compile_definitions(abe_spark_core
	PUBLIC
		MCL_USE_LLVM=1
		MCL_USE_GMP=1
		MCL_FP_BIT=384
		MCL_FR_BIT=256
		MCL_BINT_ASM_X64=0
)

target_link_directories(abe_spark_core
	PUBLIC
		${MCL_BUILD_DIR}/lib
)

target_link_libraries(abe_spark_core
	PUBLIC
		${MCL_BUILD_DIR}/lib/libabe_framework.a
		${MCL_BUILD_DIR}/lib/libmcl.a
		OpenSSL::Crypto
)

if(NOT MSVC)
	target_link_libraries(abe_spark_core PUBLIC gmp)
endif()

set_target_properties(abe_spark_core PROPERTIES
	POSITION_INDEPENDENT_CODE ON
)

add_executable(abe_spark_types_test
	test/types_crypto_test.cpp
)

target_link_libraries(abe_spark_types_test
	PRIVATE
		abe_spark_core
)

find_package(JNI REQUIRED)

add_library(abe_spark_jni SHARED
	src/abe_spark_jni.cpp
)

target_include_directories(abe_spark_jni
	PRIVATE
		${CMAKE_CURRENT_SOURCE_DIR}/include
		${JNI_INCLUDE_DIRS}
)

target_link_libraries(abe_spark_jni
	PRIVATE
		abe_spark_core
)

set_target_properties(abe_spark_jni PROPERTIES
	POSITION_INDEPENDENT_CODE ON
	OUTPUT_NAME "abe_spark_jni"
)

add_executable(abe_spark_pipeline_test
	test/pipeline_test.cpp
)

target_link_libraries(abe_spark_pipeline_test
	PRIVATE
		abe_spark_core
)

add_executable(policy_size_encrypt_demo
	test/policy_size_encrypt_demo.cpp
)

target_link_libraries(policy_size_encrypt_demo
	PRIVATE
		abe_spark_core
)

add_executable(policy_attrs_enc_dec_demo
	test/policy_attrs_enc_dec_demo.cpp
)

target_link_libraries(policy_attrs_enc_dec_demo
	PRIVATE
		abe_spark_core
)

add_executable(single_request_bench
	test/single_request_bench.cpp
)

target_link_libraries(single_request_bench
	PRIVATE
		abe_spark_core
)

set(HADOOP_HOME "$ENV{HOME}/opt/hadoop" CACHE PATH "Hadoop install for libhdfs")

set(ABE_ACCESS_BENCH_SRC "${CMAKE_CURRENT_SOURCE_DIR}/../../access-bench/abe_access_bench.cpp")
if(EXISTS "${ABE_ACCESS_BENCH_SRC}")
add_executable(abe_access_bench
	${CMAKE_CURRENT_SOURCE_DIR}/../../access-bench/abe_access_bench.cpp
	${CMAKE_CURRENT_SOURCE_DIR}/../../access-bench/hdfs_io.cpp
)

target_include_directories(abe_access_bench PRIVATE
	${CMAKE_CURRENT_SOURCE_DIR}/../../access-bench
	${HADOOP_HOME}/include
	${JNI_INCLUDE_DIRS}
)

target_link_directories(abe_access_bench PRIVATE
	${HADOOP_HOME}/lib/native
)

target_link_libraries(abe_access_bench
	PRIVATE
		abe_spark_core
		Threads::Threads
		hdfs
		${JNI_LIBRARIES}
)

set_target_properties(abe_access_bench PROPERTIES
	CXX_STANDARD 11
	CXX_STANDARD_REQUIRED ON
	RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../../access-bench/build"
)
endif()

enable_testing()
add_test(NAME abe_spark_types_test COMMAND abe_spark_types_test)
add_test(NAME abe_spark_pipeline_test COMMAND abe_spark_pipeline_test)
