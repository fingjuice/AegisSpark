#!/usr/bin/env bash
# 在 10.26.40.83-86 部署 Occlum SGX 模拟环境（无硬件 SGX 时使用 SIM 模式）
# 用法:
#   bash deploy_occlum_cluster.sh check          # 检查各节点状态
#   bash deploy_occlum_cluster.sh pack           # 在 83 打包
#   bash deploy_occlum_cluster.sh install-local  # 本机安装
#   bash deploy_occlum_cluster.sh deploy-all     # 打包并部署到 83-86
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACK_DIR="${ROOT}/dist/occlum-sim-pack"
TARBALL="${ROOT}/dist/occlum-sim-0.29.5-ubuntu20.04.tar.gz"
OCCLUM_IMAGE="occlum/occlum:0.29.5-ubuntu20.04"
NODES=(10.26.40.83 10.26.40.84 10.26.40.85 10.26.40.86)
USER="shanlicheng"
SUDO_PASS="${SUDO_PASS:-shanlicheng}"

sudo_cmd() {
  echo "${SUDO_PASS}" | sudo -S "$@" 2>/dev/null
}

run_docker() {
  if docker ps >/dev/null 2>&1; then
    docker "$@"
  elif sg docker -c "docker ps" >/dev/null 2>&1; then
    sg docker -c "docker $*"
  else
    echo "ERROR: docker not accessible for ${USER}" >&2
    return 1
  fi
}

check_node() {
  local ip="$1"
  echo "--- ${ip} ---"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "${USER}@${ip}" bash -s <<'EOF'
set +e
echo -n "hostname: "; hostname
echo -n "occlum: "; command -v occlum && occlum 2>&1 | head -1 || echo NOT_INSTALLED
echo -n "docker: "; command -v docker && docker --version 2>/dev/null || echo NOT_INSTALLED
echo -n "docker_access: "; docker ps >/dev/null 2>&1 && echo OK || echo DENIED
echo -n "docker_group: "; groups | tr ' ' '\n' | grep -qx docker && echo yes || echo no
echo -n "sgx_dev: "; ls /dev/sgx/enclave >/dev/null 2>&1 && echo hw || echo sim_only
EOF
}

ensure_docker_user() {
  local ip="$1"
  ssh -o BatchMode=yes "${USER}@${ip}" "echo '${SUDO_PASS}' | sudo -S bash -s" <<'EOF'
set -e
if ! command -v docker >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io
  systemctl enable --now docker
fi
if ! getent group docker >/dev/null; then
  groupadd docker
fi
if ! id -nG shanlicheng | grep -qw docker; then
  usermod -aG docker shanlicheng
  echo "added shanlicheng to docker group (re-login required)"
fi
EOF
}

pack_occlum() {
  mkdir -p "${ROOT}/dist"
  rm -rf "${PACK_DIR}"
  mkdir -p "${PACK_DIR}"

  echo "[pack] pulling ${OCCLUM_IMAGE} ..."
  run_docker pull "${OCCLUM_IMAGE}"

  local cid
  cid="$(run_docker create "${OCCLUM_IMAGE}" sleep infinity)"
  trap 'run_docker rm -f "${cid}" >/dev/null 2>&1 || true' EXIT

  echo "[pack] extracting /opt/occlum and /opt/intel/sgxsdk ..."
  run_docker cp "${cid}:/opt/occlum" "${PACK_DIR}/"
  run_docker cp "${cid}:/opt/intel/sgxsdk" "${PACK_DIR}/intel-sgxsdk"

  cat > "${PACK_DIR}/install.sh" <<'INSTALL'
#!/usr/bin/env bash
set -euo pipefail
PREFIX="/opt/occlum"
SDK="/opt/intel/sgxsdk"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[install] installing runtime dependencies ..."
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends libfuse2 ca-certificates wget gnupg || true

if ! grep -q intel-sgx /etc/apt/sources.list.d/intel-sgx.list 2>/dev/null; then
  if wget -qO /tmp/intel-sgx-deb.key https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key; then
    gpg --dearmor -o /usr/share/keyrings/intel-sgx.gpg /tmp/intel-sgx-deb.key || true
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/intel-sgx.gpg] https://download.01.org/intel-sgx/sgx_repo/ubuntu jammy main" > /etc/apt/sources.list.d/intel-sgx.list
    apt-get update || true
    apt-get install -y --no-install-recommends \
      libsgx-urts libsgx-enclave-common libsgx-launch libsgx-epid \
      libsgx-quote-ex libsgx-dcap-ql libsgx-ae-id-enclave libsgx-ae-qe3 \
      libsgx-dcap-default-qpl || true
  else
    echo "[install] skip apt Intel SGX packages; using bundled sgxsdk"
  fi
fi

echo "[install] copying Occlum runtime to ${PREFIX} ..."
rm -rf "${PREFIX}" "${SDK}"
mkdir -p /opt/intel
cp -a "${SCRIPT_DIR}/occlum" "${PREFIX}"
cp -a "${SCRIPT_DIR}/intel-sgxsdk" "${SDK}"

cat > /etc/profile.d/occlum-sim.sh <<'ENV'
export OCCLUM_ROOT=/opt/occlum
export INTEL_SGXSDK=/opt/intel/sgxsdk
export PATH=/opt/occlum/bin:/opt/occlum/build/bin:/usr/local/occlum/bin:/opt/intel/sgxsdk/bin:${PATH}
export LD_LIBRARY_PATH=/opt/intel/sgxsdk/sdk_libs:/opt/intel/sgxsdk/lib64:/opt/occlum/build/lib:${LD_LIBRARY_PATH:-}
export SGX_MODE=SIM
export OCCLUM=1
ENV

ln -sf "${PREFIX}/bin/occlum" /usr/local/bin/occlum

echo "[install] verifying ..."
source /etc/profile.d/occlum-sim.sh
command -v occlum
occlum 2>&1 | head -3
echo "[install] done. Use: source /etc/profile.d/occlum-sim.sh"
INSTALL
  chmod +x "${PACK_DIR}/install.sh"

  echo "[pack] creating tarball ${TARBALL} ..."
  tar -C "${ROOT}/dist" -czf "${TARBALL}" "$(basename "${PACK_DIR}")"
  run_docker rm -f "${cid}" >/dev/null 2>&1 || true
  trap - EXIT
  echo "[pack] tarball ready: ${TARBALL} ($(du -h "${TARBALL}" | awk '{print $1}'))"
}

install_local() {
  if [[ ! -f "${TARBALL}" ]]; then
    pack_occlum
  fi
  echo "[install-local] installing on $(hostname) ..."
  sudo_cmd rm -rf /tmp/occlum-sim-pack
  sudo_cmd tar -C /tmp -xzf "${TARBALL}"
  sudo_cmd bash /tmp/occlum-sim-pack/install.sh
  source /etc/profile.d/occlum-sim.sh
  command -v occlum && occlum 2>&1 | head -1
}

deploy_remote() {
  local ip="$1"
  echo "[deploy] ${ip} ..."
  ensure_docker_user "${ip}"
  scp -o BatchMode=yes "${TARBALL}" "${USER}@${ip}:/tmp/occlum-sim-pack.tar.gz"
  ssh -o BatchMode=yes "${USER}@${ip}" "echo '${SUDO_PASS}' | sudo -S bash -s" <<'EOF'
set -e
rm -rf /tmp/occlum-sim-pack
tar -C /tmp -xzf /tmp/occlum-sim-pack.tar.gz
bash /tmp/occlum-sim-pack/install.sh
EOF
  ssh -o BatchMode=yes "${USER}@${ip}" 'source /etc/profile.d/occlum-sim.sh && command -v occlum && occlum 2>&1 | head -1'
}

deploy_all() {
  pack_occlum
  for ip in "${NODES[@]}"; do
    if [[ "${ip}" == "10.26.40.83" ]]; then
      install_local
    else
      deploy_remote "${ip}"
    fi
  done
}

cmd="${1:-check}"
case "${cmd}" in
  check)
    for ip in "${NODES[@]}"; do check_node "${ip}"; done
    ;;
  pack) pack_occlum ;;
  install-local) install_local ;;
  deploy-all) deploy_all ;;
  deploy-remote)
    shift || true
    for ip in "$@"; do deploy_remote "${ip}"; done
    ;;
  *)
    echo "usage: $0 {check|pack|install-local|deploy-all|deploy-remote <ip>...}" >&2
    exit 1
    ;;
esac
