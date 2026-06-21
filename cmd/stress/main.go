package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 并发压力测试：模拟大量闲鱼助手回调瞬时涌入，验证
// 1) 捕获接口快速返回，无大面积超时
// 2) 无 "database is locked" 错误
// 3) 订单全部入库
func main() {
	const total = 200
	const concurrency = 100

	var wg sync.WaitGroup
	wg.Add(total)

	var (
		okCount    int64
		errCount   int64
		lockedSeen int64
		maxLatency int64
	)

	start := time.Now()
	sem := make(chan struct{}, concurrency)

	for i := 0; i < total; i++ {
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			body := fmt.Sprintf(`{"order_no":"STRESS-%d","message":"买家已付款，请发到 buyer%d@example.com 谢谢"}`, idx, idx)
			reqStart := time.Now()
			resp, err := http.Post("http://localhost:8080/api/order/capture", "application/json", strReader(body))
			latency := time.Since(reqStart).Milliseconds()
			atomic.StoreInt64(&maxLatency, maxInt64(atomic.LoadInt64(&maxLatency), latency))

			if err != nil {
				atomic.AddInt64(&errCount, 1)
				fmt.Printf("请求失败 idx=%d err=%v\n", idx, err)
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)

			if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
				atomic.AddInt64(&okCount, 1)
			} else {
				atomic.AddInt64(&errCount, 1)
				if contains(string(b), "locked") || contains(string(b), "BUSY") {
					atomic.AddInt64(&lockedSeen, 1)
				}
				if idx < 3 {
					fmt.Printf("非预期状态 idx=%d status=%d body=%s\n", idx, resp.StatusCode, string(b))
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// 等待 worker 把队列消费完（即使 TG 失败也会快速回写）。
	time.Sleep(2 * time.Second)

	// 校验入库数量。
	count := queryCount()

	fmt.Println("========== 压测结果 ==========")
	fmt.Printf("总请求数:      %d\n", total)
	fmt.Printf("成功捕获:      %d\n", atomic.LoadInt64(&okCount))
	fmt.Printf("失败请求:      %d\n", atomic.LoadInt64(&errCount))
	fmt.Printf("锁冲突错误:    %d\n", atomic.LoadInt64(&lockedSeen))
	fmt.Printf("最大单请求延迟: %d ms\n", atomic.LoadInt64(&maxLatency))
	fmt.Printf("总耗时:        %s (平均 %.1f ms/req)\n", elapsed, float64(elapsed.Milliseconds())/float64(total))
	fmt.Printf("数据库订单数:  %d (期望 %d)\n", count, total)

	if atomic.LoadInt64(&lockedSeen) == 0 && count == int64(total) {
		fmt.Println("✅ 结论: 无 database is locked，订单全部入库，雪崩已消除")
	} else {
		fmt.Println("❌ 结论: 仍存在问题")
	}
}

func queryCount() int64 {
	resp, err := http.Get("http://localhost:8080/api/orders?page=1&page_size=1")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	s := string(b)
	i := indexOf(s, `"total":`)
	if i < 0 {
		return -1
	}
	rest := s[i+len(`"total":`):]
	end := indexOf(rest, ",")
	if end < 0 {
		end = indexOf(rest, "}")
	}
	n, _ := strconv.ParseInt(trimSpace(rest[:end]), 10, 64)
	return n
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func contains(s, sub string) bool {
	return indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func strReader(s string) io.Reader {
	return &stringReader{s: s}
}

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
