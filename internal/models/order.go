package models

import "time"

// 订单状态常量。
const (
	StatusPending  = "pending"  // 已捕获，待通知
	StatusNotified = "notified" // 已成功发送 Telegram 通知
	StatusFailed   = "failed"   // Telegram 通知失败
)

// Order 表示一条闲鱼订单及其发货记录。
type Order struct {
	ID                int64      `json:"id"`
	BuyerEmail        string     `json:"buyer_email"`
	OrderNo           string     `json:"order_no"`
	RawMessage        string     `json:"raw_message,omitempty"`
	DownloadLink      string     `json:"download_link"`
	ExtractionCode    string     `json:"extraction_code"`
	TelegramMessageID string     `json:"telegram_message_id,omitempty"`
	Status            string     `json:"status"`
	ErrorMessage      string     `json:"error_message,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	NotifiedAt        *time.Time `json:"notified_at,omitempty"`
}
