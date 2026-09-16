#!/usr/bin/env bash
# 策略属性个数 10~100 vs 加密/解密耗时（本地 TEE 模拟，无集群）
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
OUT_DIR="$ROOT/result/policy-attrs"
BUILD="$ROOT/abe-eac/native/build"
REPS="${1:-40}"

mkdir -p "$OUT_DIR"
export ABE_SPARK_ROOT="$ROOT"
export ABE_SPARK_CONFIG="${ABE_SPARK_CONFIG:-$ROOT/abe-eac/conf/abe-spark.conf}"
export ABE_SPARK_CONF_DIR="${ABE_SPARK_CONF_DIR:-$ROOT/abe-eac/conf/keys}"
export MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
export LD_LIBRARY_PATH="${MCL_ROOT}/build-eac/lib:${LD_LIBRARY_PATH:-}"

cd "$BUILD"
cmake .. >/dev/null
cmake --build . -j"$(nproc)" --target policy_attrs_enc_dec_demo

CSV="$OUT_DIR/enc_dec_vs_attrs.csv"
"$BUILD/policy_attrs_enc_dec_demo" "$CSV" "$REPS"

cat > "$OUT_DIR/README.md" << 'EOF'
# 策略属性个数 vs 加密/解密（当前 ABE 实现）

本地 TEE 模拟路径：`abe_spark_core` → `encryptDek` / `decryptDek`。

- 自变量：策略叶子属性个数 **10, 20, …, 100**
- 策略形态：`attr:0 and attr:1 and … and attr:N-1`
- 解密用户持有全部 N 个属性
- 结果：`enc_dec_vs_attrs.csv`

说明：当前默认引擎 `cpabe-prototype` 加密主路径为解析策略 + `hash(policy)` + 一次配对/KDF；
解密为策略求值 + 一次配对。属性个数主要影响策略解析/求值与密文字符串长度，配对次数不随 N 增长。
EOF

echo
echo "==== result ===="
column -t -s, "$CSV" 2>/dev/null || cat "$CSV"
echo
echo "CSV: $CSV"
