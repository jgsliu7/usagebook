//! i18n 六道测试闸门（docs/多语言方案.md §6.4）
//! 失败信息一律列出具体 key / 字面量 / 行号，面向无 Rust 背景的翻译贡献者。
//! 闸门 4 同时守护 serve.rs 的 split_once 拆分假设（唯一内联脚本块）。

use regex::Regex;
use std::collections::BTreeMap;
use std::fmt::Write as _;

const DASHBOARD: &str = include_str!("../src/dashboard.html");
const ZH: &str = include_str!("../src/i18n/zh-CN.js");
const EN: &str = include_str!("../src/i18n/en-US.js");

/* ---------- 词法器（§6.4-5：单遍状态机，非顺序剥除） ---------- */

struct StrLit {
    content: String,
    byte_start: usize,
}

/// 单遍扫描：同时识别注释 / 单双引号串 / 模板串(含 ${} 嵌套) / 正则字面量。
/// 返回全部字符串字面量（含内容与字节起点），并把注释区间替换为空格得到 code 视图。
/// 正则识别启发式：前一有效 token ∈ {= ([ ! & | ? : ; { } => return case} 之后。
fn lex(src: &str) -> (Vec<StrLit>, String) {
    #[derive(Clone)]
    enum St {
        Code,
        Line,
        Block,
        Quo(char, usize),   // (quote, 起始下标)
        Tpl(usize),         // 起始下标
        TplCode(usize),     // ${} 内花括号深度
        Re,
    }
    let chars: Vec<char> = src.chars().collect();
    let mut code_view: Vec<char> = chars.clone();
    let mut stack: Vec<St> = vec![St::Code];
    let mut lits = Vec::new();
    let mut last_sig = ' ';
    let mut last_word = String::new();
    let mut i = 0;
    let mut char_to_byte = vec![0usize; chars.len() + 1];
    {
        let mut acc = 0usize;
        for (ci, ch) in chars.iter().enumerate() {
            char_to_byte[ci] = acc;
            acc += ch.len_utf8();
        }
        char_to_byte[chars.len()] = acc;
    }
    while i < chars.len() {
        let st = stack.last().cloned().unwrap();
        let c = chars[i];
        let next = chars.get(i + 1).copied().unwrap_or('\0');
        match st {
            St::Code | St::TplCode(_) => match c {
                '/' if next == '/' => {
                    stack.push(St::Line);
                    code_view[i] = ' ';
                    code_view[i + 1] = ' ';
                    i += 2;
                    continue;
                }
                '/' if next == '*' => {
                    stack.push(St::Block);
                    code_view[i] = ' ';
                    code_view[i + 1] = ' ';
                    i += 2;
                    continue;
                }
                '\'' | '"' => stack.push(St::Quo(c, i)),
                '`' => stack.push(St::Tpl(i)),
                '/' => {
                    let trig = "=([!&|?:;{}".contains(last_sig)
                        || last_word == "return"
                        || last_word == "case"
                        || last_word.ends_with("=>");
                    if trig {
                        stack.push(St::Re);
                    }
                }
                '{' => {
                    if let Some(St::TplCode(d)) = stack.last_mut() {
                        *d += 1;
                    }
                }
                '}' => {
                    if let Some(St::TplCode(d)) = stack.last_mut() {
                        if *d == 0 {
                            stack.pop();
                        } else {
                            *d -= 1;
                        }
                    }
                }
                _ => {}
            },
            St::Line => {
                code_view[i] = ' ';
                if c == '\n' {
                    stack.pop();
                }
            }
            St::Block => {
                code_view[i] = ' ';
                if c == '*' && next == '/' {
                    code_view[i + 1] = ' ';
                    stack.pop();
                    i += 2;
                    continue;
                }
            }
            St::Quo(q, s) => {
                if c == '\\' {
                    i += 2;
                    continue;
                }
                if c == q {
                    lits.push(StrLit {
                        content: chars[s + 1..i].iter().collect(),
                        byte_start: char_to_byte[s + 1],
                    });
                    stack.pop();
                    last_sig = q;
                }
            }
            St::Tpl(s) => {
                if c == '\\' {
                    i += 2;
                    continue;
                }
                if c == '`' {
                    lits.push(StrLit {
                        content: chars[s + 1..i].iter().collect(),
                        byte_start: char_to_byte[s + 1],
                    });
                    stack.pop();
                    last_sig = '`';
                } else if c == '$' && next == '{' {
                    stack.push(St::TplCode(0));
                    i += 2;
                    last_sig = '{';
                    continue;
                }
            }
            St::Re => {
                if c == '\\' {
                    i += 2;
                    continue;
                }
                if c == '[' {
                    while i < chars.len() && chars[i] != ']' {
                        if chars[i] == '\\' {
                            i += 1;
                        }
                        i += 1;
                    }
                }
                if c == '/' {
                    stack.pop();
                    last_sig = '/';
                }
            }
        }
        // 词尾跟踪：空白不截断(支持 "return /re/"),非字母数字截断
        if c.is_alphanumeric() || c == '_' || c == '>' || c == '=' {
            last_word.push(c);
        } else if !c.is_whitespace() {
            last_word.clear();
        }
        if !c.is_whitespace() {
            last_sig = c;
        }
        i += 1;
    }
    (lits, code_view.into_iter().collect())
}

/* ---------- 字典解析（闸门 1；解析失败即失败） ---------- */

fn parse_dict(src: &str, expect_locale: &str) -> BTreeMap<String, String> {
    let head = Regex::new(r"^I18N\['([A-Za-z-]+)'\] = \{$").unwrap();
    let item = Regex::new(r#"^\s*'([^']+)':\s*'(.*)',\s*$"#).unwrap();
    let mut map = BTreeMap::new();
    let mut locale = None;
    for (n, raw) in src.lines().enumerate() {
        let line = raw.trim_end();
        let ln = n + 1;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(c) = head.captures(line) {
            let loc = c[1].to_string();
            assert_eq!(
                loc, expect_locale,
                "字典文件 locale 声明不符({expect_locale}.js:{ln}): {line}"
            );
            locale = Some(loc);
            continue;
        }
        if line == "};" {
            continue;
        }
        let Some(cap) = item.captures(line) else {
            panic!("字典排版违约({expect_locale}.js:{ln},须为  'key': 'value', 一行一项): {line}");
        };
        assert!(
            !cap[2].contains("\\'"),
            "字典值撇号一律用排版撇号 ’(不支持 \\' 转义)({expect_locale}.js:{ln}): {line}"
        );
        assert!(
            map.insert(cap[1].to_string(), cap[2].to_string()).is_none(),
            "字典重复 key({expect_locale}.js:{ln}): {line}"
        );
    }
    assert!(locale.is_some(), "字典缺少 I18N['{expect_locale}'] 头部声明");
    map
}

fn line_of(src: &str, byte: usize) -> usize {
    src.get(..byte).map(|s| s.matches('\n').count() + 1).unwrap_or(0)
}

/// 按字符序号计行(词法器的 code_view 用等字符数空格替换注释,字符序号与原文对齐、字节序号不对齐)
fn line_of_char(src: &str, char_idx: usize) -> usize {
    src.chars().take(char_idx).filter(|c| *c == '\n').count() + 1
}

fn split_dashboard() -> (&'static str, &'static str, &'static str) {
    let (html, tail) = DASHBOARD.split_once("<script>").expect("dashboard script");
    let (js, suffix) = tail.split_once("</script>").expect("dashboard script end");
    (html, js, suffix)
}

/* ---------- 闸门 ---------- */

/// 闸门 1：key 完整性（两语言 key 集相等；字典文件解析失败即失败）
#[test]
fn gate1_dict_key_parity() {
    let zh = parse_dict(ZH, "zh-CN");
    let en = parse_dict(EN, "en-US");
    let mut msg = String::new();
    for k in zh.keys() {
        if !en.contains_key(k) {
            let _ = writeln!(msg, "  en-US.js 缺 key: {k}");
        }
    }
    for k in en.keys() {
        if !zh.contains_key(k) {
            let _ = writeln!(msg, "  zh-CN.js 缺 key: {k}");
        }
    }
    assert!(
        msg.is_empty(),
        "字典 key 不一致(zh={} en={}):\n{msg}",
        zh.len(),
        en.len()
    );
}

/// 闸门 2：引用完整性 + 受控动态 key（三类合法形态；wd.* 与 METRICS labelKey 强制入库）
#[test]
fn gate2_reference_integrity() {
    let zh = parse_dict(ZH, "zh-CN");
    let en = parse_dict(EN, "en-US");
    let mut refs: Vec<(String, usize)> = Vec::new();

    let attr = Regex::new(r#"data-i18n(?:-[a-z]+)?="([^"]+)""#).unwrap();
    for c in attr.captures_iter(DASHBOARD) {
        refs.push((c[1].to_string(), 0));
    }

    let (_html, js, _sfx) = split_dashboard();
    let (lits, code_view) = lex(js);
    assert!(!lits.is_empty(), "词法器未提取到任何字符串字面量");

    // 形态② 的 key 空间：METRICS 定义处的字面量 labelKey
    let labelkey = Regex::new(r"labelKey:\s*'([^']+)'").unwrap();
    let metric_keys: Vec<String> = labelkey
        .captures_iter(&code_view)
        .map(|c| c[1].to_string())
        .collect();
    assert!(!metric_keys.is_empty(), "未找到 METRICS labelKey 字面量声明");

    // t() 调用四类合法形态检查（在注释剔除后的 code 视图上扫描）
    let tcall = Regex::new(r"(?s)\bt\((.{0,120}?)\)").unwrap();
    let lit = Regex::new(r"^'([^']*)'").unwrap();
    let mut classified: Vec<(usize, usize)> = Vec::new(); // 已分类匹配的字符区间
    for c in tcall.captures_iter(&code_view) {
        let arg = c[1].trim_start();
        let byte_off = c.get(0).unwrap().start();
        let char_idx = code_view[..byte_off].chars().count();
        let ln = line_of_char(js, char_idx);
        classified.push((char_idx, char_idx + c.get(0).unwrap().as_str().chars().count()));
        if let Some(k) = lit.captures(arg) {
            if &k[1] == "wd." {
                continue; // 形态③：'wd.' + 表达式，key 空间由 wd.0~wd.6 强制覆盖
            }
            refs.push((k[1].to_string(), ln));
        } else if arg.starts_with("METRICS") && arg.contains(".labelKey") {
            continue; // 形态②
        } else if arg == "el.dataset.i18n" || arg == "el.dataset.i18nTitle" {
            continue; // 形态④：applyStaticI18n 的属性驱动调用(data-i18n / data-i18n-title)，key 空间由属性提取覆盖(上方已收)
        } else {
            panic!(
                "t() 首参非合法形态(dashboard.html 脚本区第 {ln} 行): t({arg}…\n\
                 合法形态: ① t('key')  ② t(METRICS[…].labelKey)  ③ t('wd.' + …)  ④ t(el.dataset.i18n / i18nTitle)"
            );
        }
    }
    // 守恒断言：原文每个 t(出现都必须落在某个已分类匹配区间内(起点或嵌套)，
    // 防止超长首参(>120 字符被截断)的调用被静默跳过
    let raw_t = Regex::new(r"\bt\(").unwrap();
    for m in raw_t.find_iter(&code_view) {
        let ci = code_view[..m.start()].chars().count();
        assert!(
            classified.iter().any(|(s, e)| ci >= *s && ci < *e),
            "t( 调用未被分类(可能首参超 120 字符被截断),字符序号 {ci}"
        );
    }
    for i in 0..7 {
        refs.push((format!("wd.{i}"), 0));
    }
    for k in &metric_keys {
        refs.push((k.clone(), 0));
    }

    let mut missing = String::new();
    for (k, ln) in &refs {
        let inzh = zh.contains_key(k);
        let inen = en.contains_key(k);
        if !inzh || !inen {
            let _ = writeln!(missing, "  key '{k}'(引用行 {ln}) 未入字典: zh={inzh} en={inen}");
        }
    }
    assert!(missing.is_empty(), "存在未入字典的引用:\n{missing}");
}

/// 闸门 3：占位符 {0}{1} 元数与集合一致（多重集比较——允许本地化换序，不允许缺/多）
#[test]
fn gate3_placeholder_parity() {
    let zh = parse_dict(ZH, "zh-CN");
    let en = parse_dict(EN, "en-US");
    let ph = Regex::new(r"\{(\d+)\}").unwrap();
    let mut msg = String::new();
    for (k, v) in &zh {
        let Some(ev) = en.get(k) else { continue };
        let mut zp: Vec<&str> = ph.captures_iter(v).map(|c| c.get(0).unwrap().as_str()).collect();
        let mut ep: Vec<&str> = ph.captures_iter(ev).map(|c| c.get(0).unwrap().as_str()).collect();
        zp.sort();
        ep.sort();
        if zp != ep {
            let _ = writeln!(msg, "  key '{k}': zh={zp:?} en={ep:?}");
        }
    }
    assert!(msg.is_empty(), "占位符不一致:\n{msg}");
}

/// 闸门 4：拆分与拼接守护（与 serve.rs 拼接逻辑同构）
#[test]
fn gate4_split_and_splice() {
    let (html, js, _sfx) = split_dashboard();
    assert!(
        !js.contains("<script") && !js.contains("</script"),
        "脚本体内含 script 标签,会破坏 serve.rs 的 split_once 拆分"
    );
    let i18n_js = format!("'use strict';const I18N={{}};{ZH}{EN}");
    let dashboard_js = format!("{i18n_js}{js}");
    assert!(
        dashboard_js.starts_with("'use strict';"),
        "拼接结果必须以 'use strict'; 开头(严格模式指令序言)"
    );
    assert!(
        dashboard_js.contains("I18N['zh-CN']") && dashboard_js.contains("I18N['en-US']"),
        "拼接结果缺语言块"
    );
    assert!(
        !html.contains("<script>"),
        "index 残余裸 <script>(引用须带 src 属性)"
    );
}

/// 闸门 5：脚本区中文字面量 = 符号白名单（万/亿/万亿）之外零 CJK
#[test]
fn gate5_no_cjk_literals_in_script() {
    let (_html, js, _sfx) = split_dashboard();
    let (lits, _code) = lex(js);
    let cjk = Regex::new("[\u{4e00}-\u{9fa5}]").unwrap();
    const WHITELIST: [&str; 3] = ["万", "亿", "万亿"];
    let mut bad = String::new();
    for lit in &lits {
        if cjk.is_match(&lit.content) && !WHITELIST.contains(&lit.content.as_str()) {
            let ln = line_of(js, lit.byte_start);
            let _ = writeln!(bad, "  脚本区第 {ln} 行含中文字面量: {:?}", lit.content);
        }
    }
    assert!(
        bad.is_empty(),
        "字典外中文字面量(文案应走 t(),计量单位可登记符号白名单):\n{bad}"
    );
}

/// 闸门 6：HTML 区静态文案挂载。剥离 <style> 后，自身直接文本节点含 CJK 的元素
/// 必须挂 data-i18n；原生 title 属性含 CJK 须挂 data-i18n-title。
/// 豁免登记：seg-unit「万/亿」按钮与语言切换「中文」按钮（单位符号/母语名）。
#[test]
fn gate6_static_text_mounted() {
    let (html, _js, _sfx) = split_dashboard();
    let style = Regex::new(r"(?s)<style.*?</style>").unwrap();
    let h = style.replace(html, "");
    let cjk = Regex::new("[\u{4e00}-\u{9fa5}]").unwrap();

    let mut stack: Vec<String> = Vec::new();
    let tok = Regex::new(r"(<[^>]+>)|([^<]+)").unwrap();
    let title_attr = Regex::new(r#"(?i)title="([^"]*)""#).unwrap();
    const EXEMPT_TXT: [&str; 2] = ["万/亿", "中文"];
    const VOID_PREFIX: [&str; 7] = ["<input", "<br", "<img", "<hr", "<meta", "<link", "<!"];
    for c in tok.captures_iter(&h) {
        if let Some(tag) = c.get(1) {
            let t = tag.as_str();
            if t.starts_with("</") {
                stack.pop();
                continue;
            }
            if t.ends_with("/>") || VOID_PREFIX.iter().any(|p| t.starts_with(p)) {
                continue;
            }
            if let Some(v) = title_attr.captures(t) {
                if cjk.is_match(&v[1]) && !t.contains("data-i18n-title") {
                    panic!("静态 title 属性含中文且未挂 data-i18n-title: {t}");
                }
            }
            stack.push(t.to_string());
        } else if let Some(txt) = c.get(2) {
            if cjk.is_match(txt.as_str()) {
                let top = stack.last().cloned().unwrap_or_default();
                let exempt = EXEMPT_TXT.iter().any(|e| txt.as_str().trim() == *e) && top.starts_with("<button");
                if !exempt && !top.contains("data-i18n") {
                    panic!(
                        "HTML 元素直接文本含中文但未挂 data-i18n(标签: {top}, 文本: {})",
                        txt.as_str().trim()
                    );
                }
            }
        }
    }
}
