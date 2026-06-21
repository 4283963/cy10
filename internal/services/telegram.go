package services

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"xianyu-relay/internal/models"
)

const telegramAPIBase = "https://api.telegram.org/bot"

// TelegramService 封装与 Telegram Bot API 的交互。
type TelegramService struct {
	botToken string
	chatID   string
	client   *http.Client
}

// NewTelegramService 创建 Telegram 通知服务。
func NewTelegramService(botToken, chatID string) *TelegramService {
	return &TelegramService{
		botToken: strings.TrimSpace(botToken),
		chatID:   strings.TrimSpace(chatID),
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

// Configured 返回服务是否已配置好 token 与 chat_id。
func (s *TelegramService) Configured() bool {
	return s.botToken != "" && s.chatID != ""
}

// sendMessageResponse 对应 Telegram sendMessage 接口的返回结构。
type sendMessageResponse struct {
	OK     bool `json:"ok"`
	Result struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
}

// SendMessage 向配置的频道/群组发送一条文本消息，返回消息 ID。
func (s *TelegramService) SendMessage(text string) (int64, error) {
	if !s.Configured() {
		return 0, fmt.Errorf("telegram 未配置 bot_token 或 chat_id")
	}

	form := url.Values{}
	form.Set("chat_id", s.chatID)
	form.Set("text", text)
	form.Set("parse_mode", "HTML")
	form.Set("disable_web_page_preview", "true")

	apiURL := telegramAPIBase + s.botToken + "/sendMessage"
	resp, err := s.client.PostForm(apiURL, form)
	if err != nil {
		return 0, fmt.Errorf("调用 telegram 接口: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result sendMessageResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("解析 telegram 返回: %w (body=%s)", err, truncate(string(body), 200))
	}
	if !result.OK {
		return 0, fmt.Errorf("telegram 接口返回错误: %s (code=%d)", result.Description, result.ErrorCode)
	}
	return result.Result.MessageID, nil
}

// SendShippingMessage 组装发货文案并发送，返回消息 ID。
func (s *TelegramService) SendShippingMessage(o *models.Order, scriptName string) (int64, error) {
	return s.SendMessage(BuildShippingMessage(o, scriptName))
}

// BuildShippingMessage 构造发货通知的 HTML 文案。
func BuildShippingMessage(o *models.Order, scriptName string) string {
	name := strings.TrimSpace(scriptName)
	if name == "" {
		name = "自动化脚本工具"
	}
	orderNo := strings.TrimSpace(o.OrderNo)
	if orderNo == "" {
		orderNo = "无"
	}

	var b strings.Builder
	b.WriteString("📦 <b>自动发货通知</b>\n\n")
	b.WriteString("🛠 商品：" + escapeHTML(name) + "\n")
	b.WriteString("👤 买家邮箱：<code>" + escapeHTML(o.BuyerEmail) + "</code>\n")
	b.WriteString("🔢 订单号：" + escapeHTML(orderNo) + "\n\n")
	b.WriteString("🔗 下载链接：\n" + escapeHTML(o.DownloadLink) + "\n\n")
	b.WriteString("🔑 提取码：<code>" + escapeHTML(o.ExtractionCode) + "</code>\n\n")
	b.WriteString("⏰ 时间：" + o.CreatedAt.Format("2006-01-02 15:04:05"))
	return b.String()
}

func escapeHTML(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
