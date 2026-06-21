package services

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// NotifyTask 是一条待发送的 Telegram 发货通知任务。
type NotifyTask struct {
	OrderID    int64
	BuyerEmail string
	notify     func(ctx context.Context) error
}

// NewNotifyTask 创建一条通知任务。notify 负责真正执行通知逻辑（发送 TG、回写状态），
// 由 handler 注入闭包，避免 dispatcher 直接依赖 repo/telegram 造成循环耦合。
func NewNotifyTask(orderID int64, buyerEmail string, notify func(ctx context.Context) error) NotifyTask {
	return NotifyTask{OrderID: orderID, BuyerEmail: buyerEmail, notify: notify}
}

// Dispatcher 是异步通知任务的调度器。
//
// 设计目标：把慢速的 Telegram API 调用从 HTTP 请求路径中彻底剥离。
// 闲鱼助手回调只需"快速入库 + 投递任务"即可立即返回，
// Telegram 通知由固定数量的 worker 串行消费，避免瞬时涌入的请求成批挂起协程，
// 同时配合仓储层的写超时与重试，杜绝 SQLite 的 "database is locked" 雪崩。
//
// 错峰限速：当一分钟内突然来了十几条订单时，全局 rateLimiter 会确保
// 两次发货动作之间至少间隔 minSendInterval 秒，把瞬时流量平滑摊开。
type Dispatcher struct {
	queue        chan NotifyTask
	workers      int
	shutdownOnce sync.Once
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc

	// 错峰限速：两次发货之间的最小全局间隔；0 表示不限速。
	minSendInterval time.Duration
	rateMu          sync.Mutex
	lastSendDoneAt  time.Time

	// 1 分钟滑动窗口入队计数，用于检测突发流量并打预警。
	burstWarnThreshold int // 每分钟入队数超过该值则打警告；0 不预警
	windowMu           sync.Mutex
	windowStart        time.Time
	windowCount        int

	enqueued      atomic.Int64
	processed     atomic.Int64
	failed        atomic.Int64
	throttledMs   atomic.Int64 // 累计因错峰等待的毫秒数
	burstWarnings atomic.Int64 // 累计触发的突发流量预警次数
}

// NewDispatcher 创建调度器。
//   - queueSize：任务缓冲队列容量，需足够大以吸收突发流量；满时投递背压。
//   - workers：并发 worker 数，Telegram 有速率限制，一般 1~4。
//   - minSendInterval：两次发货之间的最小间隔秒数（错峰核心）；传 0 不限速。
//   - burstWarnThresholdPerMin：每分钟入队任务数超过该值打预警日志；传 0 不预警。
func NewDispatcher(queueSize, workers, minSendIntervalSec, burstWarnThresholdPerMin int) *Dispatcher {
	if queueSize < 1 {
		queueSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	if minSendIntervalSec < 0 {
		minSendIntervalSec = 0
	}
	if burstWarnThresholdPerMin < 0 {
		burstWarnThresholdPerMin = 0
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		queue:              make(chan NotifyTask, queueSize),
		workers:            workers,
		shutdownCtx:        ctx,
		shutdownFunc:       cancel,
		minSendInterval:    time.Duration(minSendIntervalSec) * time.Second,
		burstWarnThreshold: burstWarnThresholdPerMin,
		windowStart:        time.Now(),
	}
}

// Start 启动 worker 池。
func (d *Dispatcher) Start() {
	intervalDesc := "不限速"
	if d.minSendInterval > 0 {
		intervalDesc = d.minSendInterval.String()
	}
	warnDesc := "不预警"
	if d.burstWarnThreshold > 0 {
		warnDesc = "≥" + itoa(d.burstWarnThreshold) + "/min"
	}
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.runWorker(i)
	}
	log.Printf("[dispatcher] 已启动 %d 个 worker，队列容量 %d，发货间隔 %s，突发预警 %s",
		d.workers, cap(d.queue), intervalDesc, warnDesc)
}

// runWorker 是单个 worker 的消费循环。
func (d *Dispatcher) runWorker(id int) {
	defer d.wg.Done()
	for {
		select {
		case <-d.shutdownCtx.Done():
			d.drainRemaining(id)
			return
		case task, ok := <-d.queue:
			if !ok {
				return
			}
			d.processWithThrottle(id, task)
		}
	}
}

// drainRemaining 在关闭阶段尽力消费尚未处理的任务（仍受错峰限速约束）。
func (d *Dispatcher) drainRemaining(workerID int) {
	for {
		select {
		case task, ok := <-d.queue:
			if !ok {
				return
			}
			log.Printf("[dispatcher] worker=%d 关闭中仍处理 order_id=%d", workerID, task.OrderID)
			d.processWithThrottle(workerID, task)
		default:
			return
		}
	}
}

// processWithThrottle 先在锁内"占据"下一个合法的发送时间槽，再在锁外 sleep 到该时刻后执行通知。
// 这是错峰机制的核心：多个 worker 通过竞争 rateMu 锁来依次占据时间槽
//
//	T0 + interval, T0 + 2*interval, T0 + 3*interval ...
//
// 从而保证任意两次发送的时间差 ≥ minSendInterval，即使 workers > 1 也不会并发触发下游。
func (d *Dispatcher) processWithThrottle(workerID int, task NotifyTask) {
	waited := d.claimSendSlot()
	if waited > 0 {
		d.throttledMs.Add(waited.Milliseconds())
	}

	d.handleTask(workerID, task)

	d.rateMu.Lock()
	d.lastSendDoneAt = time.Now()
	d.rateMu.Unlock()
}

// claimSendSlot 在锁内占据下一个合法发送时间槽，返回需要等待的时长。
// 设计要点：
//  1. 在锁内计算下一个允许发送的最早时刻 nextSendAt，并立即推进共享游标
//     (用 nextSendAt 覆盖 lastSendDoneAt)，这样后续 worker 再抢到锁时
//     看到的已经是被我推过的时间，自然落入 T+2*interval、T+3*interval ……
//  2. 时间槽的占据在锁内完成，sleep 在锁外执行，不阻塞其他 worker 抢槽。
//  3. 如果 minSendInterval=0 或关闭信号触发，直接返回 0，不等待。
func (d *Dispatcher) claimSendSlot() time.Duration {
	if d.minSendInterval <= 0 {
		return 0
	}
	d.rateMu.Lock()
	now := time.Now()
	nextSendAt := d.lastSendDoneAt.Add(d.minSendInterval)
	if nextSendAt.Before(now) {
		nextSendAt = now
	}
	// 关键：锁内立刻把游标推进到"我要占据的这个槽"，
	// 这样下一个 worker 进来时看到的 lastSendDoneAt 已经是我的槽位，
	// 它会自动被排到 T+2*interval，保证全局不并发。
	d.lastSendDoneAt = nextSendAt
	d.rateMu.Unlock()

	if !nextSendAt.After(now) {
		return 0
	}
	wait := nextSendAt.Sub(now)
	select {
	case <-time.After(wait):
		return wait
	case <-d.shutdownCtx.Done():
		return 0
	}
}

// waitForSendSlot 兼容别名（保留以备后续扩展）。
func (d *Dispatcher) waitForSendSlot() time.Duration {
	return d.claimSendSlot()
}

// handleTask 执行单条通知任务，带单任务超时保护。
func (d *Dispatcher) handleTask(workerID int, task NotifyTask) {
	if task.notify == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := task.notify(ctx); err != nil {
		d.failed.Add(1)
		log.Printf("[dispatcher] worker=%d 通知失败 order_id=%d err=%v", workerID, task.OrderID, err)
	} else {
		log.Printf("[dispatcher] worker=%d 通知完成 order_id=%d", workerID, task.OrderID)
	}
	d.processed.Add(1)
}

// recordEnqueue 对每次入队做 1 分钟滑动窗口计数，超阈值打一次预警。
func (d *Dispatcher) recordEnqueue() {
	if d.burstWarnThreshold <= 0 {
		return
	}
	d.windowMu.Lock()
	defer d.windowMu.Unlock()

	now := time.Now()
	if now.Sub(d.windowStart) >= time.Minute {
		d.windowStart = now
		d.windowCount = 0
	}
	d.windowCount++
	if d.windowCount == d.burstWarnThreshold {
		d.burstWarnings.Add(1)
		log.Printf("[dispatcher] ⚠️  突发流量预警：过去 1 分钟内已入队 %d 条发货任务，已启用错峰排队（间隔 %s）",
			d.windowCount, d.minSendInterval)
	}
}

// Enqueue 投递一条通知任务。队列满时返回 false（背压）。
func (d *Dispatcher) Enqueue(task NotifyTask) bool {
	select {
	case d.queue <- task:
		d.enqueued.Add(1)
		d.recordEnqueue()
		return true
	default:
		return false
	}
}

// EnqueueBlocking 投递任务，队列满时阻塞等待，适合入库已成功、必须投递的场景。
func (d *Dispatcher) EnqueueBlocking(task NotifyTask) bool {
	select {
	case d.queue <- task:
		d.enqueued.Add(1)
		d.recordEnqueue()
		return true
	case <-d.shutdownCtx.Done():
		return false
	}
}

// Shutdown 优雅关闭：停止接收新任务，等待所有 worker 处理完队列中的剩余任务。
func (d *Dispatcher) Shutdown(timeout time.Duration) {
	d.shutdownOnce.Do(func() {
		log.Printf("[dispatcher] 开始优雅关闭，等待剩余任务处理（最长 %s）...", timeout)
		d.shutdownFunc()
	})
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("[dispatcher] 已优雅关闭（入队=%d 处理=%d 失败=%d 错峰等待=%ds 突发预警=%d）",
			d.enqueued.Load(), d.processed.Load(), d.failed.Load(),
			d.throttledMs.Load()/1000, d.burstWarnings.Load())
	case <-time.After(timeout):
		log.Printf("[dispatcher] 关闭超时，仍有任务未完成（入队=%d 处理=%d）",
			d.enqueued.Load(), d.processed.Load())
	}
}

// Stats 返回调度器运行时统计。
func (d *Dispatcher) Stats() DispatcherStats {
	queueLen := len(d.queue)
	d.windowMu.Lock()
	winCount := d.windowCount
	winStart := d.windowStart
	d.windowMu.Unlock()
	perMin := 0
	elapsed := time.Since(winStart)
	if elapsed > 0 && elapsed <= time.Minute {
		// 换算成近似的每分钟速率
		perMin = int(float64(winCount) * float64(time.Minute) / float64(elapsed))
	} else if elapsed > time.Minute {
		perMin = 0
	} else {
		perMin = winCount * 60
	}
	return DispatcherStats{
		Workers:         d.workers,
		QueueCap:        cap(d.queue),
		QueueLen:        queueLen,
		Enqueued:        d.enqueued.Load(),
		Processed:       d.processed.Load(),
		Failed:          d.failed.Load(),
		ThrottledMs:     d.throttledMs.Load(),
		MinSendInterval: int64(d.minSendInterval / time.Second),
		BurstWarnings:   d.burstWarnings.Load(),
		EnqueuePerMin:   int64(perMin),
	}
}

// DispatcherStats 是调度器的运行时统计快照。
type DispatcherStats struct {
	Workers         int   `json:"workers"`
	QueueCap        int   `json:"queue_cap"`
	QueueLen        int   `json:"queue_len"`
	Enqueued        int64 `json:"enqueued"`
	Processed       int64 `json:"processed"`
	Failed          int64 `json:"failed"`
	ThrottledMs     int64 `json:"throttled_ms"`      // 累计因错峰等待的毫秒数
	MinSendInterval int64 `json:"min_send_interval"` // 当前配置的最小发货间隔（秒）
	BurstWarnings   int64 `json:"burst_warnings"`    // 累计触发的突发流量预警次数
	EnqueuePerMin   int64 `json:"enqueue_per_min"`   // 当前 1 分钟滑动窗口入队速率（近似）
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
