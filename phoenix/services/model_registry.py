# 版权所有 2026 X.A.I Corp.
#
# 根据 Apache 许可证 2.0 版本（"许可证"）授权；
# 除非遵守许可证，否则您不得使用此文件。

"""
模型注册表 - 管理模型权重加载和版本

支持:
- 从本地 checkpoint 加载
- 随机初始化 (开发和测试)
- 热更新检测
"""

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import haiku as hk
import jax
import jax.numpy as jnp
import numpy as np

from grok import TrainingState

logger = logging.getLogger("model_registry")


@dataclass
class ModelVersion:
    """模型版本信息"""
    version: str
    path: str
    timestamp: float
    metadata: Dict[str, Any]


class CheckpointLoader:
    """Checkpoint 加载器"""
    
    @staticmethod
    def load_from_path(checkpoint_path: str) -> Dict[str, Any]:
        """
        从路径加载 checkpoint
        
        支持格式:
        - .pkl / .pickle: Python pickle
        - .npy: NumPy 格式
        - 目录: 假设包含多个参数文件
        
        Returns:
            参数字典
        """
        path = Path(checkpoint_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Checkpoint not found: {checkpoint_path}")
        
        # 根据后缀选择加载方式
        if path.suffix == ".pkl" or path.suffix == ".pickle":
            import pickle
            with open(path, "rb") as f:
                params = pickle.load(f)
            logger.info(f"Loaded pickle checkpoint from {checkpoint_path}")
            return params
        
        elif path.suffix == ".npy":
            params = np.load(path, allow_pickle=True).item()
            logger.info(f"Loaded numpy checkpoint from {checkpoint_path}")
            return params
        
        elif path.is_dir():
            # 从目录加载多个参数文件
            params = {}
            for param_file in path.glob("*.npy"):
                key = param_file.stem
                params[key] = np.load(param_file, allow_pickle=True)
            logger.info(f"Loaded checkpoint directory from {checkpoint_path} ({len(params)} files)")
            return params
        
        else:
            raise ValueError(f"Unsupported checkpoint format: {checkpoint_path}")
    
    @staticmethod
    def convert_to_jax(params: Dict[str, Any]) -> Any:
        """将 numpy 参数转换为 JAX 格式"""
        
        def convert_value(v):
            if isinstance(v, np.ndarray):
                return jnp.array(v)
            elif isinstance(v, dict):
                return {k: convert_value(vv) for k, vv in v.items()}
            elif isinstance(v, list):
                return [convert_value(vv) for vv in v]
            else:
                return v
        
        return convert_value(params)


class ModelRegistry:
    """
    模型注册表
    
    管理:
    1. 模型版本
    2. Checkpoint 加载
    3. 热更新 (可选)
    """
    
    def __init__(self, checkpoint_path: Optional[str] = None):
        self.checkpoint_path = checkpoint_path
        self.current_version: Optional[ModelVersion] = None
        self.current_params: Optional[Any] = None
        self.last_check_time: float = 0
        self.check_interval_s: float = 60  # 检查更新间隔
        
        if checkpoint_path:
            self._load_checkpoint()
    
    def _load_checkpoint(self) -> None:
        """加载 checkpoint"""
        try:
            raw_params = CheckpointLoader.load_from_path(self.checkpoint_path)
            self.current_params = CheckpointLoader.convert_to_jax(raw_params)
            
            stat = os.stat(self.checkpoint_path)
            self.current_version = ModelVersion(
                version=str(int(stat.st_mtime)),
                path=self.checkpoint_path,
                timestamp=stat.st_mtime,
                metadata={"size_bytes": stat.st_size},
            )
            
            logger.info(f"Loaded model version {self.current_version.version} "
                       f"from {self.checkpoint_path}")
        
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            raise
    
    def get_params(self) -> Optional[Any]:
        """获取当前参数"""
        # 可选: 检查是否需要热更新
        self._check_for_update()
        return self.current_params
    
    def _check_for_update(self) -> None:
        """检查 checkpoint 是否有更新 (热更新机制)"""
        if not self.checkpoint_path:
            return
        
        current_time = time.time()
        if current_time - self.last_check_time < self.check_interval_s:
            return
        
        self.last_check_time = current_time
        
        try:
            stat = os.stat(self.checkpoint_path)
            if self.current_version and stat.st_mtime > self.current_version.timestamp:
                logger.info(f"Detected new checkpoint, reloading...")
                self._load_checkpoint()
        except Exception as e:
            logger.warning(f"Failed to check for checkpoint update: {e}")
    
    def is_ready(self) -> bool:
        """检查模型是否已加载 (或允许随机初始化)"""
        return True  # 随机初始化也视为 ready


class DummyModelRegistry(ModelRegistry):
    """
    Dummy 注册表 - 用于随机初始化场景
    
    在开发和测试环境使用，生产环境应使用真实 Checkpoint。
    """
    
    def __init__(self):
        super().__init__(checkpoint_path=None)
        logger.info("Using DummyModelRegistry (random initialization)")
    
    def get_params(self) -> Optional[Any]:
        return None  # 表示使用随机初始化
    
    def is_ready(self) -> bool:
        return True


def create_model_registry(
    checkpoint_path: Optional[str] = None,
    allow_random_init: bool = True,
) -> ModelRegistry:
    """
    工厂函数创建模型注册表
    
    Args:
        checkpoint_path: Checkpoint 路径，None 则使用随机初始化
        allow_random_init: 是否允许随机初始化
    
    Returns:
        ModelRegistry 实例
    """
    if checkpoint_path is None:
        if not allow_random_init:
            raise ValueError("checkpoint_path is required when allow_random_init=False")
        return DummyModelRegistry()
    
    return ModelRegistry(checkpoint_path)
