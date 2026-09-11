"""演示链路与旧 Phoenix 引擎链路的隔离契约。

仓库里有两套 recsys 协议：

- 演示链路：`proto/definitions/phoenix_recsys.proto`（包名 `recsys`），由
  `services.recsys_proto.load_proto_modules()` 编译加载；
- 旧引擎链路：`xai_proto` 包里的 `recsys.proto`（包名 `xai.recsys.v1`），被 `xrex` 顶层导入。

protobuf Python 只有一个进程级 descriptor pool，注册键是 proto 文件名。两套协议曾经都叫
`recsys.proto`，同进程导入即报 `duplicate file name`。本文件锁住三件事：

1. 演示链路的运行时导入闭包里不出现 `xrex` / `xai_proto`；
2. 演示 proto 注册名固定为 `phoenix_recsys.proto`，包名固定为 `recsys`；
3. 两套 proto 可以在同一进程共存。

1 和 3 必须在新解释器里验证：pytest 进程内其它测试可能已经导入过相关模块。
"""

import subprocess
import sys
from pathlib import Path

from services.recsys_proto import load_proto_modules

PHOENIX_ROOT = Path(__file__).resolve().parents[1]
ENGINE_PACKAGE_PREFIXES = ("xrex", "xai_proto")


def _demo_chain_modules() -> list[str]:
    """演示链路的运行时模块：根目录 `*.py` + `services/` 下全部可导入模块。

    按目录自动发现，新增模块自动纳入守门范围。`services/proto_gen/` 是生成代码，
    由 `load_proto_modules()` 单独覆盖；`scripts/` 依赖 `_setup_path` 只能运行不能导入，不在此列。
    """
    root_modules = [p.stem for p in PHOENIX_ROOT.glob("*.py")]
    service_modules = [
        ".".join(p.relative_to(PHOENIX_ROOT).with_suffix("").parts)
        for p in (PHOENIX_ROOT / "services").rglob("*.py")
        if p.name != "__init__.py" and "proto_gen" not in p.parts
    ]
    return sorted(root_modules + service_modules)


def _run_in_fresh_interpreter(code: str) -> subprocess.CompletedProcess:
    # 不用 check=True：CalledProcessError 的消息里没有 stderr，守门测试触发时必须能看到
    # 子进程里的原始报错（例如 duplicate file name）。
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PHOENIX_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"子进程退出码 {result.returncode}\n--- stderr ---\n{result.stderr}"
    )
    return result


def test_demo_chain_import_closure_excludes_engine_packages():
    modules = _demo_chain_modules()
    # 防止 glob 写错导致空名单静默通过。
    assert {"recsys_model", "services.grpc_gateway", "services.ranker_strategies.factory"} <= set(
        modules
    )

    code = f"""
import importlib, sys
for name in {modules!r}:
    importlib.import_module(name)
from services.recsys_proto import load_proto_modules
load_proto_modules()
leaked = sorted(
    m for m in sys.modules
    if m.split(".")[0] in {ENGINE_PACKAGE_PREFIXES!r}
)
print("\\n".join(leaked))
"""
    result = _run_in_fresh_interpreter(code)
    leaked = [line for line in result.stdout.splitlines() if line]
    assert not leaked, f"演示链路导入闭包泄漏了引擎包：{leaked}"


def test_slim_proto_registers_under_its_own_descriptor_name():
    recsys_pb2, recsys_pb2_grpc = load_proto_modules()

    assert recsys_pb2.DESCRIPTOR.name == "phoenix_recsys.proto"
    assert recsys_pb2.DESCRIPTOR.package == "recsys"
    assert hasattr(recsys_pb2_grpc, "PhoenixPredictionServiceServicer")
    assert hasattr(recsys_pb2_grpc, "PhoenixRetrievalServiceServicer")


def test_slim_and_engine_protos_coexist_in_one_process():
    code = """
from services.recsys_proto import load_proto_modules
slim, _ = load_proto_modules()
from xai_proto import recsys_pb2 as engine
names = (slim.DESCRIPTOR.name, engine.DESCRIPTOR.name)
assert names == ("phoenix_recsys.proto", "recsys.proto"), names
assert slim.DESCRIPTOR.package == "recsys"
assert engine.DESCRIPTOR.package == "xai.recsys.v1"
# 同名消息类型分别解析到各自的包，字段语义互不覆盖。
assert slim.TweetInfo.DESCRIPTOR.full_name == "recsys.TweetInfo"
assert engine.TweetInfo.DESCRIPTOR.full_name == "xai.recsys.v1.TweetInfo"
print("ok")
"""
    result = _run_in_fresh_interpreter(code)
    assert result.stdout.strip() == "ok"
