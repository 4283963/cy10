package database

import (
	"database/sql"
	"fmt"
	"time"

	"xianyu-relay/internal/models"
)

// OrderRepo 封装订单表的读写操作。
type OrderRepo struct {
	db *sql.DB
}

// NewOrderRepo 创建订单仓储。
func NewOrderRepo(db *sql.DB) *OrderRepo {
	return &OrderRepo{db: db}
}

// Create 插入一条订单记录，返回自增 ID。
func (r *OrderRepo) Create(o *models.Order) (int64, error) {
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now()
	}
	res, err := r.db.Exec(
		`INSERT INTO orders
			(buyer_email, order_no, raw_message, download_link, extraction_code, status, error_message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		o.BuyerEmail, o.OrderNo, o.RawMessage, o.DownloadLink, o.ExtractionCode,
		o.Status, o.ErrorMessage, o.CreatedAt.Format(time.RFC3339),
	)
	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return id, nil
}

// UpdateNotification 更新某订单的通知结果。
func (r *OrderRepo) UpdateNotification(id int64, status, telegramMessageID, errMsg string) error {
	notifiedAt := ""
	if status == models.StatusNotified {
		notifiedAt = time.Now().Format(time.RFC3339)
	}
	_, err := r.db.Exec(
		`UPDATE orders SET status = ?, telegram_message_id = ?, error_message = ?, notified_at = ? WHERE id = ?`,
		status, telegramMessageID, errMsg, notifiedAt, id,
	)
	if err != nil {
		return fmt.Errorf("update notification: %w", err)
	}
	return nil
}

// List 分页查询订单（最新在前）。
func (r *OrderRepo) List(limit, offset int) ([]models.Order, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := r.db.Query(
		`SELECT id, buyer_email, order_no, download_link, extraction_code,
		        telegram_message_id, status, error_message, created_at, notified_at
		   FROM orders
		  ORDER BY id DESC
		  LIMIT ? OFFSET ?`,
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	var orders []models.Order
	for rows.Next() {
		var (
			o            models.Order
			orderNo      sql.NullString
			telegramMsg  sql.NullString
			errMsg       sql.NullString
			createdAtStr string
			notifiedAt   sql.NullString
		)
		if err := rows.Scan(
			&o.ID, &o.BuyerEmail, &orderNo, &o.DownloadLink, &o.ExtractionCode,
			&telegramMsg, &o.Status, &errMsg, &createdAtStr, &notifiedAt,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}
		o.OrderNo = orderNo.String
		o.TelegramMessageID = telegramMsg.String
		o.ErrorMessage = errMsg.String
		if t, err := parseTime(createdAtStr); err == nil {
			o.CreatedAt = t
		}
		if notifiedAt.Valid {
			if t, err := parseTime(notifiedAt.String); err == nil {
				o.NotifiedAt = &t
			}
		}
		orders = append(orders, o)
	}
	return orders, rows.Err()
}

// Count 返回订单总数。
func (r *OrderRepo) Count() (int64, error) {
	var count int64
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count orders: %w", err)
	}
	return count, nil
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %s", s)
}
