"""gRPC 网关监听地址的启动契约测试。"""

import sys
from types import SimpleNamespace

import services.grpc_gateway as gateway


class _FakeServer:
    def __init__(self):
        self.address = None
        self.started = False

    def add_insecure_port(self, address):
        self.address = address
        return 1

    def start(self):
        self.started = True

    def wait_for_termination(self):
        return None


def test_serve_defaults_to_loopback_and_allows_explicit_host(monkeypatch):
    """默认只绑定本机，同时保留显式绑定远程地址的能力。"""
    servers = []

    def fake_server(_executor):
        server = _FakeServer()
        servers.append(server)
        return server

    fake_grpc = SimpleNamespace(server=fake_server)
    monkeypatch.setitem(sys.modules, "grpc", fake_grpc)
    monkeypatch.setattr(gateway, "load_proto_modules", lambda: (object(), SimpleNamespace(
        add_PhoenixPredictionServiceServicer_to_server=lambda *_: None,
        add_PhoenixRetrievalServiceServicer_to_server=lambda *_: None,
    )))
    monkeypatch.setattr(gateway, "EmbeddingTables", lambda *_: object())
    monkeypatch.setattr(gateway, "RankerEngine", lambda *_: SimpleNamespace(model_version="ranker"))
    monkeypatch.setattr(
        gateway,
        "RetrievalEngine",
        lambda *_: SimpleNamespace(model_version="retrieval"),
    )
    monkeypatch.setattr(gateway, "create_servicers", lambda *_: (object(), object()))

    gateway.serve(port=50123)
    assert servers[-1].address == "127.0.0.1:50123"
    assert servers[-1].started

    gateway.serve(port=50124, host="0.0.0.0")
    assert servers[-1].address == "0.0.0.0:50124"
