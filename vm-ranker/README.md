# vm-ranker

可选的二次重排服务：Value Model 打分 + DPP 多样性。默认不进演示链路。

home-mixer 要同时设置 `HOME_MIXER_ENABLE_VM_RANKER=1` 和 `VM_RANKER_GRPC_ADDR` 才会装配。缺地址时主链继续跑，只打一条告警。

默认 gRPC 端口是 `9090`，和 home-mixer 的 metrics HTTP 端口撞车。HTTP 默认 `--http-port 8080`，和 Thunder 空 HTTP 口也可能撞。演示已经在跑时，换一个 gRPC 口：

```bash
cargo run -p xai-vm-ranker -- --grpc-port 50054
```

然后：

```bash
HOME_MIXER_MODE=demo \
HOME_MIXER_ENABLE_VM_RANKER=1 \
VM_RANKER_GRPC_ADDR=http://localhost:50054 \
cargo run -p home-mixer
```

协议在 `proto/definitions/vm_ranker.proto`（`VmRankerService.Rank`）。开关和超时见 [docs/home-mixer/07-config-and-params.md](../docs/home-mixer/07-config-and-params.md)。
