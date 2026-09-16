#!/usr/bin/env bash
# 恶意工作者攻击防御基准：跑实验 → 出论文图
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXP="$ROOT/abe-eac/experiments/malicious_worker"
OUT="${ABE_DEFENSE_OUT:-$ROOT/result/malicious-worker}"
mkdir -p "$OUT"

MODE="${1:-quick}"  # quick | full

export PYTHONUNBUFFERED=1
cd "$EXP"

if [[ "$MODE" == "full" ]]; then
  echo "=== FULL paper run (saturate capacity + 10k target reference) ==="
  python3 test_malicious_worker_attack.py \
    --out-dir "$OUT" \
    --scenario-requests 5000 \
    --target-rps 10000 \
    --duration 5 \
    --threads 96 \
    --payload-bytes 4096 \
    --attack-pcts "0,5,10,20,30,40,50" \
    --acl-rtt-us 800 \
    --saturate
else
  echo "=== QUICK smoke / draft figures ==="
  python3 test_malicious_worker_attack.py --quick --saturate --out-dir "$OUT"
fi

python3 plot_defense_experiment.py --data-dir "$OUT" --out-dir "$OUT/figures"

echo
echo "Artifacts:"
ls -la "$OUT"
ls -la "$OUT/figures" 2>/dev/null || true
