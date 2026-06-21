package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xianyu-relay/internal/config"
	"xianyu-relay/internal/database"
	"xianyu-relay/internal/models"
	"xianyu-relay/internal/services"
	"xianyu-relay/internal/utils"

	"github.com/gin-gonic/gin"
)

// OrderHandler 处理订单捕获与发货历史查询。
type OrderHandler struct {
	repo       *database.OrderRepo
	telegram   *services.TelegramService
	dispatcher *services.Dispatcher
	cfg        *config.Config
}

// NewOrderHandler 创建订单处理器。
// dispatcher 用于把慢速的 Telegram 通知从请求路径中异步剥离。
func NewOrderHandler(repo *database.OrderRepo, telegram *services.TelegramService, dispatcher *services.Dispatcher, cfg *config.Config) *OrderHandler {
	return &OrderHandler{repo: repo, telegram: telegram, dispatcher: dispatcher, cfg: cfg}
}

// captureRequest 是一个“尽量宽松”的请求结构，
// 兼容闲鱼助手可能推送的各种字段名；未识别字段会被纳入原始文本一并搜索邮箱。
type captureRequest struct {
	OrderNo    string                 `json:"order_no"`
	OrderID    string                 `json:"order_id"`
	TradeNo    string                 `json:"trade_no"`
	Email      string                 `json:"email"`
	BuyerEmail string                 `json:"buyer_email"`
	Buyer      string                 `json:"buyer"`
	Message    string                 `json:"message"`
	Remark     string                 `json:"remark"`
	Memo       string                 `json:"memo"`
	Content    string                 `json:"content"`
	Text       string                 `json:"text"`
	Title      string                 `json:"title"`
	Extra      map[string]interface{} `json:"extra"`
	Raw        map[string]interface{} `json:"raw"`
}

// Capture 订单捕获接口：接收闲鱼助手付款回调，抠除邮箱并入库，投递异步通知任务后立即返回。
//
// 关键设计：请求路径中只做"读 body + 提取邮箱 + 写库"这类快速本地操作，
// 慢速的 Telegram API 调用交由 dispatcher worker 异步处理，
// 避免瞬时涌入的回调把 Gin 协程成批挂起、进而拖垮 SQLite 写锁。
// 路由: POST /api/order/capture
func (h *OrderHandler) Capture(c *gin.Context) {
	if !h.checkCaptureToken(c) {
		c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "error": "invalid or missing capture token"})
		return
	}

	rawBody, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "cannot read request body"})
		return
	}
	bodyStr := string(rawBody)

	var req captureRequest
	if len(rawBody) > 0 {
		_ = json.Unmarshal(rawBody, &req)
	}

	// 把所有可能含邮箱的字段以及原始 JSON 文本拼到一起，统一搜索。
	candidates := []string{
		req.Email, req.BuyerEmail, req.Buyer, req.OrderNo, req.OrderID, req.TradeNo,
		req.Message, req.Remark, req.Memo, req.Content, req.Text, req.Title,
	}
	if req.Extra != nil {
		if b, err := json.Marshal(req.Extra); err == nil {
			candidates = append(candidates, string(b))
		}
	}
	if req.Raw != nil {
		if b, err := json.Marshal(req.Raw); err == nil {
			candidates = append(candidates, string(b))
		}
	}
	candidates = append(candidates, bodyStr)

	email := utils.ExtractEmail(strings.Join(candidates, "\n"))
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": "buyer email not found in message",
		})
		return
	}

	orderNo := firstNonEmpty(req.OrderNo, req.OrderID, req.TradeNo)
	order := &models.Order{
		BuyerEmail:     email,
		OrderNo:        orderNo,
		RawMessage:     truncateForDB(bodyStr, 4000),
		DownloadLink:   h.cfg.Shipping.DownloadLink,
		ExtractionCode: h.cfg.Shipping.ExtractionCode,
		Status:         models.StatusPending,
		CreatedAt:      time.Now(),
	}

	// 快速写库（短事务 + 重试），释放写锁后立即投递异步任务。
	id, err := h.repo.Create(order)
	if err != nil {
		log.Printf("[capture] 创建订单失败 email=%s err=%v", email, err)
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": fmt.Sprintf("db error: %v", err)})
		return
	}
	order.ID = id

	// 投递异步通知任务，Telegram 调用不再阻塞请求。
	task := services.NewNotifyTask(id, email, func(ctx context.Context) error {
		return h.processNotify(ctx, order)
	})
	if !h.dispatcher.EnqueueBlocking(task) {
		// 通常仅在服务关闭时发生。
		log.Printf("[capture] 投递通知任务失败（dispatcher 已关闭）order_id=%d", id)
		if uerr := h.repo.UpdateNotification(id, models.StatusFailed, "", "dispatcher unavailable"); uerr != nil {
			log.Printf("[capture] 回写失败状态出错 order_id=%d err=%v", id, uerr)
		}
	}

	// 立即返回，不等待 Telegram 结果；前端列表页轮询即可看到状态从 pending->notified。
	c.JSON(http.StatusAccepted, gin.H{
		"code":        202,
		"id":          id,
		"status":      models.StatusPending,
		"buyer_email": email,
		"message":     "订单已捕获，正在异步发货",
	})
}

// processNotify 由 dispatcher worker 调用：发送 Telegram 通知并回写最终状态。
// 这是原同步路径里的逻辑，被完整迁移到异步侧。
func (h *OrderHandler) processNotify(ctx context.Context, order *models.Order) error {
	msgID, err := h.telegram.SendShippingMessage(order, h.cfg.Shipping.ScriptName)
	if err != nil {
		log.Printf("[notify] telegram 通知失败 order_id=%d email=%s err=%v", order.ID, order.BuyerEmail, err)
		if uerr := h.repo.UpdateNotification(order.ID, models.StatusFailed, "", truncateForDB(err.Error(), 1000)); uerr != nil {
			log.Printf("[notify] 回写失败状态出错 order_id=%d err=%v", order.ID, uerr)
		}
		return err
	}

	msgIDStr := strconv.FormatInt(msgID, 10)
	if err := h.repo.UpdateNotification(order.ID, models.StatusNotified, msgIDStr, ""); err != nil {
		log.Printf("[notify] 回写成功状态出错 order_id=%d err=%v", order.ID, err)
	}
	log.Printf("[notify] 发货完成 order_id=%d email=%s tg_msg_id=%d", order.ID, order.BuyerEmail, msgID)
	return nil
}

// List 发货历史列表接口（分页）。
// 路由: GET /api/orders?page=1&page_size=20
func (h *OrderHandler) List(c *gin.Context) {
	page := config.AsInt(c.DefaultQuery("page", "1"), 1)
	if page < 1 {
		page = 1
	}
	pageSize := config.AsInt(c.DefaultQuery("page_size", "20"), 20)
	if pageSize < 1 || pageSize > 200 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	orders, err := h.repo.List(pageSize, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": err.Error()})
		return
	}
	total, err := h.repo.Count()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": err.Error()})
		return
	}

	if orders == nil {
		orders = []models.Order{}
	}
	c.JSON(http.StatusOK, gin.H{
		"code":       200,
		"page":       page,
		"page_size":  pageSize,
		"total":      total,
		"orders":     orders,
		"dispatcher": h.dispatcher.Stats(),
	})
}

// checkCaptureToken 校验捕获接口的访问令牌；未配置 token 时视为放行。
func (h *OrderHandler) checkCaptureToken(c *gin.Context) bool {
	expected := strings.TrimSpace(h.cfg.Server.CaptureToken)
	if expected == "" {
		return true
	}
	got := c.GetHeader("X-Capture-Token")
	if got == "" {
		got = c.Query("token")
	}
	return got == expected
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func truncateForDB(s string, max int) string {
	if len(s) > max {
		return s[:max]
	}
	return s
}
