package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"xianyu-relay/internal/models"
)

// 写操作单次尝试的超时，事务必须在此内提交，避免长期持有写锁。
const writeTimeout = 2 * time.Second

// 读操作超时，防止慢查询拖垮列表接口。
const readTimeout = 3 * time.Second

// 锁冲突最大重试次数（SQLite busy 时退避重试）。
const maxBusyRetries = 4

// OrderRepo 封装订单表的读写操作。
type OrderRepo struct {
	db *sql.DB
}

// NewOrderRepo 创建订单仓储。
func NewOrderRepo(db *sql.DB) *OrderRepo {
	return &OrderRepo{db: db}
}

// Create 插入一条订单记录，返回自增 ID。
// 所有写操作带超时与锁冲突重试，确保事务快速提交、及时释放写锁。
func (r *OrderRepo) Create(o *models.Order) (int64, error) {
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now()
	}
	var id int64
	err := r.execWithRetry(func(ctx context.Context) error {
		res, err := r.db.ExecContext(ctx,
			`INSERT INTO orders
				(buyer_email, order_no, raw_message, download_link, extraction_code, status, error_message, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			o.BuyerEmail, o.OrderNo, o.RawMessage, o.DownloadLink, o.ExtractionCode,
			o.Status, o.ErrorMessage, o.CreatedAt.Format(time.RFC3339),
		)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}
	return id, nil
}

// UpdateNotification 更新某订单的通知结果。
func (r *OrderRepo) UpdateNotification(id int64, status, telegramMessageID, errMsg string) error {
	notifiedAt := ""
	if status == models.StatusNotified {
		notifiedAt = time.Now().Format(time.RFC3339)
	}
	return r.execWithRetry(func(ctx context.Context) error {
		_, err := r.db.ExecContext(ctx,
			`UPDATE orders SET status = ?, telegram_message_id = ?, error_message = ?, notified_at = ? WHERE id = ?`,
			status, telegramMessageID, errMsg, notifiedAt, id,
		)
		return err
	})
}

// execWithRetry 在带超时的 context 中执行写操作，遇到 SQLite 锁冲突时退避重试。
// 这是抵御 "database is locked" 的核心：短超时保证不会长期挂起，
// 重试则在瞬时锁竞争时自动恢复，而不是直接把错误抛给上层。
func (r *OrderRepo) execWithRetry(fn func(ctx context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < maxBusyRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(20*(1<<attempt)) * time.Millisecond) // 40ms, 80ms, 160ms 退避
		}
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		err := fn(ctx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if !isBusyError(err) {
			return err
		}
	}
	return fmt.Errorf("sqlite busy after %d retries: %w", maxBusyRetries, lastErr)
}

// isBusyError 判断是否为 SQLite 锁冲突错误（SQLITE_BUSY）。
func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "SQLITE_BUSY") ||
		strings.Contains(msg, "busy")
}

// List 分页查询订单（最新在前）。
func (r *OrderRepo) List(limit, offset int) ([]models.Order, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	rows, err := r.db.QueryContext(ctx,
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
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var count int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM orders`).Scan(&count); err != nil {
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
