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
// 匹配语义（对齐上游 `viewer_muted_keyword_filter` 的行为测试）：
//   - 整词匹配：关键词必须对齐 token 边界，`art` 不命中 `party`。
//   - 连续有序短语：`crypto scam` 不命中 `crypto is great, scam artists are bad`。
//   - 标点、`@` 作分隔符：`bitcoin` 命中 `(bitcoin is volatile)` 和 `@bitcoin`。
//   - 话题标签不对称：`widget` 同时命中 `widget` 和 `#widget`；`#launch` 只命中 `#launch`。
//   - CJK 整段成词：CJK 连续段落作为一个 token，`京都` 不命中 `東京都に行く`。
//   - Emoji 等符号自成一个 token，可被单独设为屏蔽关键词。
//
// 与上游的已记录差异（U1，替代实现能力边界）：
//   - 上游做完整 Unicode 规范化；本地只折叠 Latin-1 附加区的变音符号
//     （见 `fold_diacritic`），拉丁扩展 A 区及其他书写系统的附加符号未折叠。
//   - 上游把 URL 识别为整体并可替换为占位符；本地按标点切分 URL。

/// 判断字符是否属于 CJK 书写系统。
///
/// 这些书写系统词间不加空格，因此一整段连续 CJK 字符构成一个 token，
/// 与上游 `whole token only` 的匹配语义一致。
fn is_cjk(c: char) -> bool {
    matches!(c,
        '\u{3040}'..='\u{30FF}'     // 平假名、片假名
        | '\u{3400}'..='\u{4DBF}'   // CJK 扩展 A
        | '\u{4E00}'..='\u{9FFF}'   // CJK 基本区
        | '\u{AC00}'..='\u{D7AF}'   // 谚文音节
        | '\u{F900}'..='\u{FAFF}'   // CJK 兼容表意文字
        | '\u{20000}'..='\u{2FA1F}' // CJK 扩展 B–F
    )
}

/// 把 Latin-1 附加区的小写变音字母折叠为 ASCII 基字母。
///
/// 调用方已完成小写化，因此只需覆盖小写码位。
fn fold_diacritic(c: char) -> Option<&'static str> {
    Some(match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => "a",
        'æ' => "ae",
        'ç' => "c",
        'è' | 'é' | 'ê' | 'ë' => "e",
        'ì' | 'í' | 'î' | 'ï' => "i",
        'ð' => "d",
        'ñ' => "n",
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' => "o",
        'ù' | 'ú' | 'û' | 'ü' => "u",
        'ý' | 'ÿ' => "y",
        'þ' => "th",
        'ß' => "ss",
        _ => return None,
    })
}

/// 判断字符是否只作分隔符，不产生 token。
fn is_separator(c: char) -> bool {
    c.is_whitespace()
        || c.is_ascii_punctuation()
        || matches!(c,
            '\u{2000}'..='\u{206F}'   // 通用标点
            | '\u{3000}'..='\u{303F}' // CJK 符号与标点
        )
}

/// 单个 token 是否命中单个关键词 token。
///
/// 话题标签在 token 中保留前导 `#`，由此表达上游的不对称语义：
/// 普通关键词同时命中正文词和话题标签，带 `#` 的关键词只命中话题标签。
fn token_matches(keyword: &str, text: &str) -> bool {
    match keyword.strip_prefix('#') {
        Some(tag) => text.strip_prefix('#') == Some(tag),
        None => keyword == text || text.strip_prefix('#') == Some(keyword),
    }
}

/// 分词结果
///
/// 表示一段文本经过分词后的 token 序列。
/// 每个 token 是原始文本的一个有意义的子串。
#[derive(Clone, Debug)]
pub struct TokenSequence {
    /// 分词后的 token 列表（已小写化；话题标签保留前导 `#`）
    pub tokens: Vec<String>,
}

impl TokenSequence {
    /// 本序列是否包含与 `keyword` 连续、有序、逐 token 对齐的片段。
    pub fn contains_keyword_sequence(&self, keyword: &Self) -> bool {
        !keyword.tokens.is_empty()
            && self.tokens.windows(keyword.tokens.len()).any(|window| {
                window
                    .iter()
                    .zip(&keyword.tokens)
                    .all(|(text, kw)| token_matches(kw, text))
            })
    }
}

/// 推文分词器
///
/// 将推文文本和屏蔽关键词分词为 TokenSequence。
/// 关键词与正文使用同一套分词规则，保证两侧 token 边界一致。
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
        let mut tokens: Vec<String> = Vec::new();
        let mut current = String::new();
        let mut current_cjk = false;
        let mut pending_hash = false;

        for c in text.to_lowercase().chars() {
            if c == '#' {
                push_token(&mut tokens, &mut current);
                pending_hash = true;
                continue;
            }

            let folded = fold_diacritic(c);
            if folded.is_none() && !c.is_alphanumeric() {
                push_token(&mut tokens, &mut current);
                pending_hash = false;
                // 空白和标点只作分隔符；其余符号（如 emoji）自成一个 token。
                if !is_separator(c) {
                    tokens.push(c.to_string());
                }
                continue;
            }

            // CJK 与非 CJK 之间是 token 边界，`京都tour` 切为 `京都` + `tour`。
            let cjk = is_cjk(c);
            if !current.is_empty() && cjk != current_cjk {
                push_token(&mut tokens, &mut current);
            }
            if current.is_empty() {
                current_cjk = cjk;
                if pending_hash {
                    current.push('#');
                    pending_hash = false;
                }
            }
            match folded {
                Some(base) => current.push_str(base),
                None => current.push(c),
            }
        }
        push_token(&mut tokens, &mut current);

        TokenSequence { tokens }
    }
}

fn push_token(tokens: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        tokens.push(std::mem::take(current));
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
        self.user_mutes
            .muted_sequences
            .iter()
            .any(|muted| tweet_tokens.contains_keyword_sequence(muted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(text: &str) -> Vec<String> {
        TweetTokenizer::new().tokenize(text).tokens
    }

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

    #[test]
    fn punctuation_and_mentions_are_separators() {
        assert_eq!(
            tokens("Buy bitcoin! It's great!!!"),
            ["buy", "bitcoin", "it", "s", "great"]
        );
        assert_eq!(
            tokens("(bitcoin is volatile)"),
            ["bitcoin", "is", "volatile"]
        );
        assert_eq!(
            tokens("Hey @exampleuser check"),
            ["hey", "exampleuser", "check"]
        );
    }

    #[test]
    fn hashtags_keep_their_marker() {
        assert_eq!(
            tokens("#Widget launch event"),
            ["#widget", "launch", "event"]
        );
        assert_eq!(tokens("#launch"), ["#launch"]);
    }

    #[test]
    fn plain_keyword_matches_hashtag_but_not_the_reverse() {
        let tokenizer = TweetTokenizer::new();
        let hashtag_text = tokenizer.tokenize("#widget launch event");
        let plain_text = tokenizer.tokenize("support launch movement");

        assert!(hashtag_text.contains_keyword_sequence(&tokenizer.tokenize("widget")));
        assert!(!plain_text.contains_keyword_sequence(&tokenizer.tokenize("#launch")));
        assert!(tokenizer
            .tokenize("#LAUNCH day")
            .contains_keyword_sequence(&tokenizer.tokenize("#launch")));
    }

    #[test]
    fn latin1_diacritics_fold_to_ascii_bases() {
        assert_eq!(
            tokens("I love café culture"),
            ["i", "love", "cafe", "culture"]
        );
        assert_eq!(tokens("café"), tokens("cafe"));
        assert_eq!(tokens("piñata"), ["pinata"]);
    }

    #[test]
    fn cjk_runs_form_one_token_and_split_at_script_boundaries() {
        assert_eq!(tokens("東京都に行くのが楽しみ"), ["東京都に行くのが楽しみ"]);
        assert_eq!(
            tokens("I visited 京都 last week"),
            ["i", "visited", "京都", "last", "week"]
        );
        assert_eq!(tokens("京都tour"), ["京都", "tour"]);
    }

    #[test]
    fn cjk_keyword_does_not_match_a_longer_surrounding_run() {
        let tokenizer = TweetTokenizer::new();
        let keyword = tokenizer.tokenize("京都");

        assert!(tokenizer
            .tokenize("I visited 京都 last week")
            .contains_keyword_sequence(&keyword));
        assert!(!tokenizer
            .tokenize("東京都に行くのが楽しみ")
            .contains_keyword_sequence(&keyword));
    }

    #[test]
    fn emoji_is_a_standalone_token_while_punctuation_only_separates() {
        assert_eq!(
            tokens("great news 🚀 today"),
            ["great", "news", "🚀", "today"]
        );
        assert_eq!(tokens("行くぞ、京都。"), ["行くぞ", "京都"]);

        let tokenizer = TweetTokenizer::new();
        assert!(tokenizer
            .tokenize("great news 🚀 today")
            .contains_keyword_sequence(&tokenizer.tokenize("🚀")));
    }

    #[test]
    fn empty_keyword_never_matches() {
        let tokenizer = TweetTokenizer::new();
        assert!(!tokenizer
            .tokenize("any content")
            .contains_keyword_sequence(&tokenizer.tokenize("   ")));
    }
}
