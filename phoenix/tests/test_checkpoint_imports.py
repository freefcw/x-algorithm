"""checkpoint 模块必须能独立导入，否则单测与隔离验证都做不了。

`import orbax.checkpoint` 依赖 `xai_checkpointing.fix_jax` 先把 jax 0.8.1 移除的
`jax.lib.xla_extension.XlaRuntimeError` 映射回 `jax.errors.JaxRuntimeError`。这个导入看起来
未被使用，容易被当成冗余删掉；一旦删掉，只有先导入 `xai_checkpointing.load` 的调用方还能工作。

必须用新解释器：同一进程里只要有任何测试先导入过 `xai_checkpointing.load`，补丁就已生效，
在本进程内断言会永远通过。
"""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    ["xrex.utils.checkpointing", "xrex.train.checkpoint_write"],
)
def test_checkpoint_module_imports_standalone(module):
    subprocess.run([sys.executable, "-c", f"import {module}"], check=True)
