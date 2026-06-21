package utils

import "regexp"

// emailRegex 匹配标准邮箱地址。忽略大小写。
var emailRegex = regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`)

// ExtractEmail 从任意文本中抠除第一个匹配到的邮箱地址。
// 用于从闲鱼助手推送的、格式不固定的消息中提取买家邮箱。
// 若未匹配到则返回空字符串。
func ExtractEmail(text string) string {
	if text == "" {
		return ""
	}
	return emailRegex.FindString(text)
}

// ExtractEmails 返回文本中所有不重复的邮箱地址（保持出现顺序）。
func ExtractEmails(text string) []string {
	if text == "" {
		return nil
	}
	matches := emailRegex.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(matches))
	result := make([]string, 0, len(matches))
	for _, m := range matches {
		lower := toLowerASCII(m)
		if !seen[lower] {
			seen[lower] = true
			result = append(result, m)
		}
	}
	return result
}

func toLowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
