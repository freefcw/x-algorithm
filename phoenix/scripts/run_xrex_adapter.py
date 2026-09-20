from __future__ import annotations

import argparse
import json
from concurrent import futures
from pathlib import Path

import grpc

import _setup_path  # noqa: F401
from services.model_contract import IDENTITY_MAPPING_VERSION
from services.recsys_proto import load_proto_modules
from services.xrex_adapter import PhoenixAdapter


def main() -> None:
    parser = argparse.ArgumentParser(description="Home Mixer Phoenix-contract adapter for xrex")
    parser.add_argument("--listen", default="[::]:50053")
    parser.add_argument("--xrex-address", default="localhost:50054")
    parser.add_argument("--model-version", required=True)
    parser.add_argument(
        "--identity-contract",
        required=True,
        help="build_training_inputs.py 产出的 training_input_metadata.json",
    )
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args()

    try:
        identity_contract = json.loads(Path(args.identity_contract).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        parser.error(f"无法读取 --identity-contract: {exc}")
    if identity_contract.get("identity_mapping_version") != IDENTITY_MAPPING_VERSION:
        parser.error("--identity-contract 的 identity_mapping_version 不受支持")
    identity_mapping_sha256 = identity_contract.get("identity_mapping_sha256")
    if not isinstance(identity_mapping_sha256, str):
        parser.error("--identity-contract 缺少 identity_mapping_sha256")

    _, recsys_grpc = load_proto_modules()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.workers))
    adapter = PhoenixAdapter(
        args.xrex_address,
        args.model_version,
        identity_mapping_sha256,
    )
    recsys_grpc.add_PhoenixPredictionServiceServicer_to_server(adapter, server)
    recsys_grpc.add_PhoenixRetrievalServiceServicer_to_server(adapter, server)
    server.add_insecure_port(args.listen)
    server.start()
    print(f"Phoenix xrex adapter listening on {args.listen}; xrex={args.xrex_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
