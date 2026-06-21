package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"xianyu-relay/internal/config"
	"xianyu-relay/internal/database"
	"xianyu-relay/internal/handlers"
	"xianyu-relay/internal/services"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	db, err := database.Init(cfg.Database.Path)
	if err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	telegramSvc := services.NewTelegramService(cfg.Telegram.BotToken, cfg.Telegram.ChatID)
	if !telegramSvc.Configured() {
		log.Printf("⚠️  Telegram 未配置 bot_token/chat_id，发货通知将失败；请在 config.yaml 或环境变量中配置")
	} else {
		log.Printf("✅ Telegram 已配置，目标 chat_id=%s", cfg.Telegram.ChatID)
	}

	// 异步通知调度器：把慢速 Telegram 调用从请求路径剥离，防止回调涌入时协程成批挂起、SQLite 写锁雪崩。
	// 同时支持错峰限速：每分钟来了十几单时，每隔 min_send_interval 秒才发一个。
	dispatcher := services.NewDispatcher(
		cfg.Dispatcher.QueueSize,
		cfg.Dispatcher.Workers,
		cfg.Dispatcher.MinSendInterval,
		cfg.Dispatcher.BurstWarnThresholdPerMin,
	)
	dispatcher.Start()
	defer dispatcher.Shutdown(time.Duration(cfg.Dispatcher.ShutdownTimeout) * time.Second)

	orderRepo := database.NewOrderRepo(db)
	orderHandler := handlers.NewOrderHandler(orderRepo, telegramSvc, dispatcher, cfg)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// 前端静态资源与首页
	r.Static("/static", "./web")
	r.GET("/", func(c *gin.Context) {
		c.File("./web/index.html")
	})

	// 健康检查
	r.GET("/api/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"code":       200,
			"status":     "ok",
			"telegram":   telegramSvc.Configured(),
			"dispatcher": dispatcher.Stats(),
			"time":       time.Now().Format(time.RFC3339),
		})
	})

	// 核心业务路由
	api := r.Group("/api")
	{
		api.POST("/order/capture", orderHandler.Capture) // 订单捕获接口
		api.GET("/orders", orderHandler.List)            // 发货历史列表
	}

	srv := &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: r,
	}

	// 优雅启停
	go func() {
		log.Printf("🚀 闲鱼中转系统已启动: http://localhost:%s", cfg.Server.Port)
		log.Printf("   捕获接口: POST /api/order/capture")
		log.Printf("   发货历史: GET  /api/orders")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("服务启动失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("收到退出信号，正在关闭服务...")

	// 先停止接收新的 HTTP 请求，再让 dispatcher 排空剩余通知任务，最后关库。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP 强制关闭: %v", err)
	}
	log.Println("服务已退出")
}
