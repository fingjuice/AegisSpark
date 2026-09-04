cmake_minimum_required(VERSION 3.10)
project(sgx_pyspark_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(ABE_SPARK_ROOT "/home/shanlicheng/test-benchmark/ABE-Spark" CACHE PATH "ABE-Spark root")
set(MCL_ROOT "/home/shanlicheng/mcl" CACHE PATH "mcl library root")
set(MCL_BUILD_DIR "${MCL_ROOT}/build-eac" CACHE PATH "mcl build directory")

find_package(OpenSSL REQUIRED)

add_library(abe_spark_ffi SHARED
	abe_spark_ffi.cpp
)

target_include_directories(abe_spark_ffi
	PRIVATE
		${CMAKE_CURRENT_SOURCE_DIR}
		${ABE_SPARK_ROOT}/abe-eac/native/include
		${MCL_ROOT}/ABE-Framework/include
		${MCL_ROOT}/include
		${MCL_ROOT}/src
)

target_compile_definitions(abe_spark_ffi
	PRIVATE
		MCL_USE_LLVM=1
		MCL_USE_GMP=1
		MCL_FP_BIT=384
		MCL_FR_BIT=256
		MCL_BINT_ASM_X64=0
)

target_link_libraries(abe_spark_ffi
	PRIVATE
		${ABE_SPARK_ROOT}/abe-eac/native/build/libabe_spark_core.a
		${MCL_BUILD_DIR}/lib/libabe_framework.a
		${MCL_BUILD_DIR}/lib/libmcl.a
		OpenSSL::Crypto
		gmp
)

set_target_properties(abe_spark_ffi PROPERTIES
	POSITION_INDEPENDENT_CODE ON
	OUTPUT_NAME "sgx_pyspark_ffi"
)
