"""
路径初始化模块 — 将项目根目录加入 Python 路径。

每个 scripts/ 下的入口脚本在导入核心模块前，只需：
    import _setup_path  # noqa: F401
"""

import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
