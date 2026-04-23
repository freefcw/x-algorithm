// S2S (Service-to-Service) 安全认证配置
//
// 替代原始被阉割的 S2S 认证模块。
//
// 原始功能说明：
// X 内部微服务之间使用双向 TLS (mTLS) 进行身份认证。
// 每个服务持有一个由内部 CA 签发的证书，用于：
//   1. 加密服务间通信
//   2. 验证调用方身份（防止未授权访问）
//   3. 签名请求（防篡改）
//
// 证书体系包含三个路径：
//   - chain: CA 根证书链（验证对方证书的信任链）
//   - crt: 本服务的证书（向对方证明自己身份）
//   - key: 本服务的私钥（签名/解密）
//
// 当前使用占位路径。生产环境应替换为实际的 mTLS 证书路径，
// 或者改用你平台的认证方案（JWT、API Key 等）。

use std::path::PathBuf;

lazy_static::lazy_static! {
    /// CA 证书链路径
    /// 用于验证上游服务证书的可信性
    pub static ref S2S_CHAIN_PATH: PathBuf = PathBuf::from("/etc/pki/tls/certs/s2s-chain.pem");

    /// 客户端证书路径
    /// Home Mixer 的身份证书，向上游服务证明自己的身份
    pub static ref S2S_CRT_PATH: PathBuf = PathBuf::from("/etc/pki/tls/certs/s2s-cert.pem");

    /// 客户端私钥路径
    /// Home Mixer 的私钥，用于 TLS 握手
    pub static ref S2S_KEY_PATH: PathBuf = PathBuf::from("/etc/pki/tls/private/s2s-key.pem");
}
