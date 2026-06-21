package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // 纯 Go 实现的 SQLite 驱动，无需 CGO
)

// Init 打开（必要时创建目录并建表）SQLite 数据库。
func Init(path string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}

	// 使用事务相关的 PRAGMA，提升并发与持久化表现。
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// SQLite 单连接写更稳，避免锁冲突。
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	return db, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    buyer_email         TEXT    NOT NULL,
    order_no            TEXT    DEFAULT '',
    raw_message         TEXT    DEFAULT '',
    download_link       TEXT    DEFAULT '',
    extraction_code     TEXT    DEFAULT '',
    telegram_message_id TEXT    DEFAULT '',
    status              TEXT    NOT NULL DEFAULT 'pending',
    error_message       TEXT    DEFAULT '',
    created_at          TEXT    NOT NULL,
    notified_at         TEXT    DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_orders_email      ON orders(buyer_email);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
`
