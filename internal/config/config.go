package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 是整个中转系统的运行时配置。
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Database   DatabaseConfig   `yaml:"database"`
	Telegram   TelegramConfig   `yaml:"telegram"`
	Shipping   ShippingConfig   `yaml:"shipping"`
	Dispatcher DispatcherConfig `yaml:"dispatcher"`
}

// ServerConfig 描述 HTTP 服务相关参数。
type ServerConfig struct {
	Port         string `yaml:"port"`
	CaptureToken string `yaml:"capture_token"` // 可选：闲鱼助手请求时携带的鉴权令牌
}

// DatabaseConfig 描述 SQLite 数据库文件路径。
type DatabaseConfig struct {
	Path string `yaml:"path"`
}

// TelegramConfig 描述 Telegram 机器人相关参数。
type TelegramConfig struct {
	BotToken string `yaml:"bot_token"` // 机器人 token
	ChatID   string `yaml:"chat_id"`   // 目标频道/群组/用户，如 @your_channel
}

// ShippingConfig 描述发货内容（下载链接与提取码）。
type ShippingConfig struct {
	DownloadLink   string `yaml:"download_link"`
	ExtractionCode string `yaml:"extraction_code"`
	ScriptName     string `yaml:"script_name"` // 脚本名称，用于通知文案
}

// DispatcherConfig 描述异步通知任务调度器（worker 池）参数。
// 这是抗并发雪崩的核心：Telegram 慢调用由固定数量 worker 串行消费，
// 不占用 HTTP 请求协程，也不会让 SQLite 写事务长期挂起。
// 同时支持全局错峰限速：每分钟来了十几单时，自动每隔 min_send_interval 秒发一个。
type DispatcherConfig struct {
	QueueSize                int `yaml:"queue_size"`                   // 任务缓冲队列容量，需足够大以吸收突发流量
	Workers                  int `yaml:"workers"`                      // 并发 worker 数，Telegram 侧有速率限制，一般 1~4
	ShutdownTimeout          int `yaml:"shutdown_timeout"`             // 优雅关闭时等待剩余任务的最长秒数
	MinSendInterval          int `yaml:"min_send_interval"`            // 两次发货之间的最小间隔秒数（错峰核心）；设 0 则不限速
	BurstWarnThresholdPerMin int `yaml:"burst_warn_threshold_per_min"` // 每分钟入队任务数超过该值时打预警日志；设 0 则不预警
}

// Load 从指定路径加载配置文件。若文件不存在则使用默认值。
// 敏感字段支持通过环境变量覆盖，环境变量优先级高于配置文件。
func Load(path string) (*Config, error) {
	cfg := &Config{
		Server:     ServerConfig{Port: "8080"},
		Database:   DatabaseConfig{Path: "./data/orders.db"},
		Shipping:   ShippingConfig{ScriptName: "自动化脚本工具"},
		Dispatcher: DispatcherConfig{QueueSize: 1000, Workers: 2, ShutdownTimeout: 10, MinSendInterval: 5, BurstWarnThresholdPerMin: 10},
	}

	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	// 默认值兜底
	if strings.TrimSpace(cfg.Server.Port) == "" {
		cfg.Server.Port = "8080"
	}
	if strings.TrimSpace(cfg.Database.Path) == "" {
		cfg.Database.Path = "./data/orders.db"
	}
	if cfg.Dispatcher.QueueSize <= 0 {
		cfg.Dispatcher.QueueSize = 1000
	}
	if cfg.Dispatcher.Workers <= 0 {
		cfg.Dispatcher.Workers = 2
	}
	if cfg.Dispatcher.ShutdownTimeout <= 0 {
		cfg.Dispatcher.ShutdownTimeout = 10
	}
	if cfg.Dispatcher.MinSendInterval < 0 {
		cfg.Dispatcher.MinSendInterval = 0
	}
	if cfg.Dispatcher.BurstWarnThresholdPerMin < 0 {
		cfg.Dispatcher.BurstWarnThresholdPerMin = 0
	}

	applyEnvOverrides(cfg)

	if abs, err := filepath.Abs(cfg.Database.Path); err == nil {
		cfg.Database.Path = abs
	}

	return cfg, nil
}

// applyEnvOverrides 用环境变量覆盖敏感/运行时配置，避免密钥落盘。
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("RELAY_SERVER_PORT"); v != "" {
		cfg.Server.Port = v
	}
	if v := os.Getenv("RELAY_CAPTURE_TOKEN"); v != "" {
		cfg.Server.CaptureToken = v
	}
	if v := os.Getenv("RELAY_DB_PATH"); v != "" {
		cfg.Database.Path = v
	}
	if v := os.Getenv("TELEGRAM_BOT_TOKEN"); v != "" {
		cfg.Telegram.BotToken = v
	}
	if v := os.Getenv("TELEGRAM_CHAT_ID"); v != "" {
		cfg.Telegram.ChatID = v
	}
	if v := os.Getenv("RELAY_DOWNLOAD_LINK"); v != "" {
		cfg.Shipping.DownloadLink = v
	}
	if v := os.Getenv("RELAY_EXTRACTION_CODE"); v != "" {
		cfg.Shipping.ExtractionCode = v
	}
	if v := os.Getenv("RELAY_SCRIPT_NAME"); v != "" {
		cfg.Shipping.ScriptName = v
	}
	if v := os.Getenv("RELAY_MIN_SEND_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Dispatcher.MinSendInterval = n
		}
	}
	if v := os.Getenv("RELAY_BURST_WARN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Dispatcher.BurstWarnThresholdPerMin = n
		}
	}
}

// AsInt 将字符串解析为 int，解析失败返回默认值。
func AsInt(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
