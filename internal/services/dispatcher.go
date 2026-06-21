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
type Dispatcher struct {
	queue        chan NotifyTask
	workers      int
	shutdownOnce sync.Once
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc

	enqueued  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
}

// NewDispatcher 创建调度器。queueSize 为任务缓冲容量，workers 为并发 worker 数。
// workers 不宜过大（Telegram 侧有速率限制），一般 1~4 即可；
// queueSize 应足够大以吸收突发流量，超出后投递将被阻塞（背压）。
func NewDispatcher(queueSize, workers int) *Dispatcher {
	if queueSize < 1 {
		queueSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		queue:        make(chan NotifyTask, queueSize),
		workers:      workers,
		shutdownCtx:  ctx,
		shutdownFunc: cancel,
	}
}

// Start 启动 worker 池。
func (d *Dispatcher) Start() {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.runWorker(i)
	}
	log.Printf("[dispatcher] 已启动 %d 个 worker，队列容量 %d", d.workers, cap(d.queue))
}

// runWorker 是单个 worker 的消费循环。
func (d *Dispatcher) runWorker(id int) {
	defer d.wg.Done()
	for {
		select {
		case <-d.shutdownCtx.Done():
			// 优雅关闭：排空队列中剩余任务。
			d.drainRemaining(id)
			return
		case task, ok := <-d.queue:
			if !ok {
				return
			}
			d.handleTask(id, task)
		}
	}
}

// drainRemaining 在关闭阶段尽力消费尚未处理的任务。
func (d *Dispatcher) drainRemaining(workerID int) {
	for {
		select {
		case task, ok := <-d.queue:
			if !ok {
				return
			}
			log.Printf("[dispatcher] worker=%d 关闭中仍处理 order_id=%d", workerID, task.OrderID)
			d.handleTask(workerID, task)
		default:
			return
		}
	}
}

// handleTask 执行单条通知任务，带单任务超时保护。
func (d *Dispatcher) handleTask(workerID int, task NotifyTask) {
	if task.notify == nil {
		return
	}
	// 单任务超时，避免个别慢请求长期占用 worker。
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

// Enqueue 投递一条通知任务。队列满时返回 false（背压）。
func (d *Dispatcher) Enqueue(task NotifyTask) bool {
	select {
	case d.queue <- task:
		d.enqueued.Add(1)
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
		log.Printf("[dispatcher] 已优雅关闭（入队=%d 处理=%d 失败=%d）",
			d.enqueued.Load(), d.processed.Load(), d.failed.Load())
	case <-time.After(timeout):
		log.Printf("[dispatcher] 关闭超时，仍有任务未完成（入队=%d 处理=%d）",
			d.enqueued.Load(), d.processed.Load())
	}
}

// Stats 返回调度器运行时统计。
func (d *Dispatcher) Stats() DispatcherStats {
	return DispatcherStats{
		Workers:   d.workers,
		QueueCap:  cap(d.queue),
		QueueLen:  len(d.queue),
		Enqueued:  d.enqueued.Load(),
		Processed: d.processed.Load(),
		Failed:    d.failed.Load(),
	}
}

// DispatcherStats 是调度器的运行时统计快照。
type DispatcherStats struct {
	Workers   int   `json:"workers"`
	QueueCap  int   `json:"queue_cap"`
	QueueLen  int   `json:"queue_len"`
	Enqueued  int64 `json:"enqueued"`
	Processed int64 `json:"processed"`
	Failed    int64 `json:"failed"`
}
