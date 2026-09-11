# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
phoenix_recsys.proto Python 桩代码加载器。

gRPC 网关需要 `proto/definitions/phoenix_recsys.proto` 编译出的 Python 代码。
为避免把生成代码提交进仓库，这里在首次使用（或 proto 文件更新后）
用 grpcio-tools 自动生成到 `services/proto_gen/`，该目录已被 gitignore。

文件名刻意不叫 `recsys.proto`：旧 Phoenix 引擎链路（`xai_proto` 包）的协议也叫
`recsys.proto`。protobuf Python 只有一个进程级 descriptor pool，注册名相同、内容不同
的两个 proto 同时导入会直接报 `duplicate file name`。用不同文件名让两套协议可以在
同一进程共存，互不干扰。

用法:
    from services.recsys_proto import load_proto_modules
    recsys_pb2, recsys_pb2_grpc = load_proto_modules()
"""

from pathlib import Path

_SERVICES_DIR = Path(__file__).resolve().parent
_GEN_DIR = _SERVICES_DIR / "proto_gen"
# 仓库布局：<repo>/proto/definitions/phoenix_recsys.proto 与 <repo>/phoenix/services/
_PROTO_PATH = _SERVICES_DIR.parent.parent / "proto" / "definitions" / "phoenix_recsys.proto"
_PB2_MODULE = "phoenix_recsys_pb2"
_PB2_GRPC_MODULE = "phoenix_recsys_pb2_grpc"


def _needs_regen() -> bool:
    pb2 = _GEN_DIR / f"{_PB2_MODULE}.py"
    if not pb2.exists():
        return True
    if _PROTO_PATH.stat().st_mtime > pb2.stat().st_mtime:
        return True
    return _gencode_incompatible(pb2)


def _gencode_incompatible(pb2: Path) -> bool:
    """gencode 比当前 protobuf runtime 新时必须重建，否则导入直接报
    VersionError（例如依赖解析把 runtime 降级后残留旧产物）。"""
    import re

    from google.protobuf import __version__ as runtime_version

    match = re.search(
        r"Protobuf Python Version: (\d+)\.(\d+)", pb2.read_text(errors="replace")
    )
    if match is None:
        return True
    gen_major, gen_minor = int(match.group(1)), int(match.group(2))
    parts = runtime_version.split(".")
    rt_major, rt_minor = int(parts[0]), int(parts[1])
    return (gen_major, gen_minor) > (rt_major, rt_minor)


def _generate() -> None:
    try:
        from grpc_tools import protoc
    except ImportError as exc:
        raise ImportError(
            "生成 gRPC 桩代码需要 grpcio-tools，请先执行: uv sync --group service"
        ) from exc

    _GEN_DIR.mkdir(exist_ok=True)
    (_GEN_DIR / "__init__.py").touch()

    rc = protoc.main(
        [
            "protoc",
            f"-I{_PROTO_PATH.parent}",
            f"--python_out={_GEN_DIR}",
            f"--grpc_python_out={_GEN_DIR}",
            str(_PROTO_PATH),
        ]
    )
    if rc != 0:
        raise RuntimeError(f"{_PROTO_PATH.name} 编译失败 (exit code {rc})")

    # 生成的 *_grpc.py 使用绝对导入 `import phoenix_recsys_pb2`，
    # 改成包内相对导入，避免污染 sys.path。
    grpc_file = _GEN_DIR / f"{_PB2_GRPC_MODULE}.py"
    text = grpc_file.read_text()
    text = text.replace(
        f"import {_PB2_MODULE} as", f"from . import {_PB2_MODULE} as"
    )
    grpc_file.write_text(text)


def load_proto_modules():
    """返回 (recsys_pb2, recsys_pb2_grpc)，必要时先自动生成。

    返回值命名沿用 `recsys_pb2`，因为对调用方而言它就是精简链路的 recsys 协议；
    真正的模块名是 `services.proto_gen.phoenix_recsys_pb2`。
    """
    if not _PROTO_PATH.exists():
        raise FileNotFoundError(
            f"找不到 proto 定义: {_PROTO_PATH}\n"
            "gRPC 网关需要完整仓库布局（proto/ 与 phoenix/ 同级）。"
        )
    if _needs_regen():
        _generate()

    from services.proto_gen import phoenix_recsys_pb2 as recsys_pb2
    from services.proto_gen import phoenix_recsys_pb2_grpc as recsys_pb2_grpc

    return recsys_pb2, recsys_pb2_grpc
