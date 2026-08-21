// 帖子文本处理模块 (Post Text)
//
// 替代原始 `xai_post_text` 私有 crate。
//
// 原始功能说明：
// xai_post_text 提供推文文本的分词和匹配功能，
// 用于实现 ViewerMutedKeywordFilter（屏蔽关键词过滤器）。
//
// 在 Home Mixer 管道中的作用链：
//   用户设置屏蔽关键词 → ViewerMutedKeywordFilter 拿到关键词列表
//   → TweetTokenizer 分词 → UserMutes 构建匹配规则
//   → MatchTweetGroup 检查每条帖子文本是否命中
//   → 命中则从候选集中移除
//
// 原始文本分词器的实现考虑了以下因素：
//   - Unicode 规范化（NFC/NFKC）
//   - 特殊字符处理（@mentions、#hashtags、URLs）
//   - 大小写折叠
//   - 语种特定的分词策略（CJK 按字符，Latin 按空格）
//
// 当前使用简化的空格分词 + 大小写折叠 stub 实现。
// TODO: 如果你平台有 CJK 内容，建议集成 jieba 或类似分词库

/// 分词结果
///
/// 表示一段文本经过分词后的 token 序列。
/// 每个 token 是原始文本的一个有意义的子串。
#[derive(Clone, Debug)]
pub struct TokenSequence {
    /// 分词后的 token 列表（已小写化）
    pub tokens: Vec<String>,
}

impl TokenSequence {
    pub fn contains_keyword_sequence(&self, keyword: &Self) -> bool {
        !keyword.tokens.is_empty()
            && self
                .tokens
                .windows(keyword.tokens.len())
                .any(|window| window == keyword.tokens.as_slice())
    }
}

/// 推文分词器
///
/// 将推文文本和屏蔽关键词分词为 TokenSequence。
///
/// 原始 X 实现中使用定制的分词器，处理了 Twitter 特有的文本格式：
///   - @user_mentions → 识别并保留
///   - #hashtags → 去除 # 符号后作为 token
///   - URLs → 去除或替换为占位符
///   - Emoji → 保留为独立 token
///   - 连续标点/空白 → 折叠
///
/// 当前简化版使用空格分词 + 小写化。
#[derive(Default)]
pub struct TweetTokenizer;

impl TweetTokenizer {
    pub fn new() -> Self {
        Self
    }

    /// 将文本分词为 TokenSequence
    ///
    /// # Arguments
    /// * `text` - 原始文本（帖子文本或屏蔽关键词）
    ///
    /// # Returns
    /// 分词后的 TokenSequence
    pub fn tokenize(&self, text: &str) -> TokenSequence {
        let tokens: Vec<String> = text
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        TokenSequence { tokens }
    }
}

/// 用户屏蔽关键词集合
///
/// 将用户的多个屏蔽关键词编译为匹配规则集，
/// 用于高效批量检查帖子文本。
pub struct UserMutes {
    /// 每个屏蔽关键词的分词结果
    muted_sequences: Vec<TokenSequence>,
}

impl UserMutes {
    pub fn new(sequences: Vec<TokenSequence>) -> Self {
        Self {
            muted_sequences: sequences,
        }
    }
}

/// 推文组匹配器
///
/// 检查推文文本是否命中任何屏蔽关键词。
/// 使用朴素的子序列匹配策略。
pub struct MatchTweetGroup {
    user_mutes: UserMutes,
}

impl MatchTweetGroup {
    pub fn new(user_mutes: UserMutes) -> Self {
        Self { user_mutes }
    }

    /// 检查推文文本是否命中任何屏蔽关键词
    ///
    /// # Arguments
    /// * `tweet_tokens` - 推文文本的分词结果
    ///
    /// # Returns
    /// true 如果命中任何屏蔽关键词
    pub fn matches(&self, tweet_tokens: &TokenSequence) -> bool {
        for muted_seq in &self.user_mutes.muted_sequences {
            if muted_seq.tokens.is_empty() {
                continue;
            }
            // 检查 muted_seq 的所有 token 是否都出现在 tweet_tokens 中
            let all_found = muted_seq
                .tokens
                .iter()
                .all(|muted_token| tweet_tokens.tokens.iter().any(|t| t.contains(muted_token)));
            if all_found {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple() {
        let tokenizer = TweetTokenizer::new();
        let seq = tokenizer.tokenize("Hello World");
        assert_eq!(seq.tokens, vec!["hello", "world"]);
    }

    #[test]
    fn test_muted_keyword_match() {
        let tokenizer = TweetTokenizer::new();
        let muted = vec![tokenizer.tokenize("spam")];
        let user_mutes = UserMutes::new(muted);
        let matcher = MatchTweetGroup::new(user_mutes);

        let tweet = tokenizer.tokenize("This is spam content");
        assert!(matcher.matches(&tweet));

        let clean_tweet = tokenizer.tokenize("This is good content");
        assert!(!matcher.matches(&clean_tweet));
    }

    #[test]
    fn keyword_sequence_requires_contiguous_exact_tokens() {
        let tokenizer = TweetTokenizer::new();
        let tweet = tokenizer.tokenize("breaking news from the art desk");

        assert!(tweet.contains_keyword_sequence(&tokenizer.tokenize("breaking news")));
        assert!(!tweet.contains_keyword_sequence(&tokenizer.tokenize("breaking desk")));
        assert!(!tokenizer
            .tokenize("party time")
            .contains_keyword_sequence(&tokenizer.tokenize("art")));
    }
}
