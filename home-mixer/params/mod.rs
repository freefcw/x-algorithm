// 参数模块，目录布局对齐上游 `47c1bcd` home-mixer/params/。
//
// - `config.rs`：进程级常量（上游真值 + 本地环境适配值）。
// - `param.rs`：上游 feature-switch 参数默认值的本地常量对应。
//
// 统一 re-export 保持既有 `crate::params::X` 调用点不变。

pub mod config;
pub mod param;

pub use config::*;
pub use param::*;
