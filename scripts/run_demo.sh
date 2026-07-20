#!/usr/bin/env bash
# 一键演示：把完整推荐链路（Thunder + Phoenix + Home Mixer）在本机跑起来，
# 并请求一次推荐 Feed，验证端到端返回非空结果。
#
# 用法：
#   ./scripts/run_demo.sh          # 跑一次演示后自动清理所有进程
#   ./scripts/run_demo.sh --keep   # 演示后保持三个服务运行（Ctrl+C 退出）
#
# 前置条件：
#   - 已安装 Rust 工具链（cargo）与 uv
#   - 已执行过 cd phoenix && uv sync --group service
#
# 端口占用：50051 (home-mixer) / 50052 (thunder) / 50053 (phoenix gateway)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

KEEP_RUNNING=false
[[ "${1:-}" == "--keep" ]] && KEEP_RUNNING=true

PIDS=()

cleanup() {
    echo ""
    echo "[demo] 正在停止演示进程..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo "[demo] 已清理。"
}
trap cleanup EXIT INT TERM

wait_for_port() {
    # 注意：macOS 自带 bash 3.2 对 "$var" 后紧跟中文字符的解析有 bug，
    # 变量一律用 ${var} 花括号形式。
    local port=$1 name=$2 max_wait=${3:-90}
    for _ in $(seq 1 "${max_wait}"); do
        if lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "[demo] ${name} 已就绪（端口 ${port}）"
            return 0
        fi
        sleep 1
    done
    echo "[demo] 错误：${name} 在 ${max_wait}s 内未监听端口 ${port}，查看日志：/tmp/demo-${name}.log"
    exit 1
}

check_port_free() {
    local port=$1 name=$2
    if lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "[demo] 错误：端口 ${port} 已被占用（${name} 需要它）。先停掉占用进程再重试。"
        exit 1
    fi
}

echo "[demo] === X 推荐算法本地演示 ==="

command -v cargo >/dev/null || { echo "[demo] 缺少 cargo，请先安装 Rust 工具链"; exit 1; }
command -v uv >/dev/null || { echo "[demo] 缺少 uv，请先安装：https://docs.astral.sh/uv/"; exit 1; }

check_port_free 50051 home-mixer
check_port_free 50052 thunder
check_port_free 50053 phoenix-gateway

echo "[demo] 1/5 编译 Rust 服务（首次需要几分钟）..."
cargo build -p thunder -p home-mixer --bin thunder --bin home-mixer --bin demo-client

echo "[demo] 2/5 启动 Phoenix gRPC 网关（模型服务，加载 JAX 需要约 1 分钟）..."
(cd phoenix && exec uv run scripts/run_grpc_gateway.py --corpus-size 1000) \
    >/tmp/demo-phoenix-gateway.log 2>&1 &
PIDS+=($!)

echo "[demo] 3/5 启动 Thunder（演示模式：内置 200 条模拟帖子，无需 Kafka）..."
RUST_LOG=info cargo run -q -p thunder -- --demo-seed-posts 200 --grpc-port 50052 \
    >/tmp/demo-thunder.log 2>&1 &
PIDS+=($!)

wait_for_port 50052 thunder 60
wait_for_port 50053 phoenix-gateway 120

echo "[demo] 4/5 启动 Home Mixer（演示模式 + 连接 Thunder 与 Phoenix）..."
RUST_LOG=info \
HOME_MIXER_DEMO=1 \
THUNDER_GRPC_ADDR=http://localhost:50052 \
PHOENIX_PREDICT_GRPC_ADDR=http://localhost:50053 \
PHOENIX_RETRIEVAL_GRPC_ADDR=http://localhost:50053 \
cargo run -q -p home-mixer \
    >/tmp/demo-home-mixer.log 2>&1 &
PIDS+=($!)

wait_for_port 50051 home-mixer 60
sleep 2

echo "[demo] 5/5 请求推荐 Feed..."
echo ""
cargo run -q -p home-mixer --bin demo-client

echo ""
echo "[demo] 端到端链路验证通过。"
echo "[demo] 服务日志：/tmp/demo-{thunder,home-mixer,phoenix-gateway}.log"

if $KEEP_RUNNING; then
    echo "[demo] --keep 模式：服务保持运行，按 Ctrl+C 退出并清理。"
    echo "[demo] 可以再次请求：cargo run -p home-mixer --bin demo-client"
    wait
fi
