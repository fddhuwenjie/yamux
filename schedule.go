// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"sync"
	"time"
)

const (
	// priorityMin 与 priorityMax 限定流发送等级的取值范围，
	// 数值越大越优先发送。
	priorityMin = 0
	priorityMax = 7

	// priorityControl 是会话级控制帧（Ping、GoAway 等）的固定等级，
	// 高于所有数据帧，保证心跳等控制消息不被大数据流量阻塞。
	priorityControl = priorityMax + 1

	// priorityAgingPeriod 是防饥饿老化步长：低等级帧在队列中每多等待
	// priorityAgingPeriod 次发送，其有效等级提升 1 级，直至 priorityMax。
	// 这样持续繁忙的高等级流每发送若干帧就会让等待中的低等级帧获得
	// 一次同等级（按 FIFO 更靠前）的发送机会，使低等级流持续前进。
	priorityAgingPeriod = 4

	// sendQueueCapacity 限制等待发送（含尚未被发送协程取出）的帧数量，
	// 与优先级模式关闭时 sendCh 的缓冲深度保持一致。
	sendQueueCapacity = 64
)

// sendScheduler 是优先级模式开启时 sendLoop 使用的发送调度器。
// 它替代先进先出的 sendCh：所有待发送帧进入同一队列，由唯一的发送
// 协程通过 pop 选取下一帧。选帧规则为“有效等级高者优先，同级按就绪
// 先后（单调序号）”，低等级帧随等待时间（按已发送帧数计量）老化升级，
// 避免被持续繁忙的高等级流长期饿死。
//
// 队列深度由 tokens 信号量限定：put 先取令牌（队满时阻塞，可被超时或
// 会话关闭唤醒），pop 取出帧后立即归还，语义与向带缓冲 channel 发送
// 一致。发送协程在空队列上通过 sync.Cond 等待，等待期间不持有锁，
// 不会阻塞生产者入队。
type sendScheduler struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []*sendReady

	// tokens 是容量为队列深度的信号量，预先填满；每放入一帧消耗一个，
	// 帧被 pop 取出后归还，从而把待发送帧数量限制在容量之内。
	tokens chan struct{}

	// nextSeq 为进入队列的帧分配单调递增的就绪序号。
	nextSeq uint64

	// served 记录 pop 已经取出的帧总数，作为老化计算的时钟。
	served uint64

	// closed 在会话关闭时置位并广播，唤醒所有等待者。
	closed bool
}

// newSendScheduler 创建一个容量为 capacity 的发送调度器。
func newSendScheduler(capacity int) *sendScheduler {
	q := &sendScheduler{
		items:  make([]*sendReady, 0, capacity),
		tokens: make(chan struct{}, capacity),
	}
	for i := 0; i < capacity; i++ {
		q.tokens <- struct{}{}
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// put 将一帧加入调度队列。队列满时阻塞，直到有空位、超时或会话关闭。
// timeout 为写超时通道，shutdown 为会话关闭通道。
func (q *sendScheduler) put(ready *sendReady, timeout <-chan time.Time, shutdown <-chan struct{}) error {
	// 在队满情况下阻塞等待空位；等待期间不持有任何互斥锁。
	select {
	case <-q.tokens:
	case <-shutdown:
		return ErrSessionShutdown
	case <-timeout:
		return ErrConnectionWriteTimeout
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		q.releaseToken()
		return ErrSessionShutdown
	}
	ready.seq = q.nextSeq
	q.nextSeq++
	ready.queuedAt = q.served
	q.items = append(q.items, ready)
	q.mu.Unlock()

	// 唤醒唯一的发送协程；队列此前为空时它一定在等待。
	q.cond.Signal()
	return nil
}

// pop 选取并移除下一帧：有效等级最高者优先，同等级按就绪序号（FIFO）。
// 队列为空时阻塞；调度器关闭后返回 nil（尚未发送的帧与 FIFO 模式一样
// 被放弃，各发送方通过 shutdownCh 得到 ErrSessionShutdown）。
func (q *sendScheduler) pop() *sendReady {
	q.mu.Lock()
	for len(q.items) == 0 {
		if q.closed {
			q.mu.Unlock()
			return nil
		}
		q.cond.Wait()
	}

	best := 0
	bestLevel := q.items[0].effectiveLevel(q.served)
	for i := 1; i < len(q.items); i++ {
		level := q.items[i].effectiveLevel(q.served)
		if level > bestLevel || (level == bestLevel && q.items[i].seq < q.items[best].seq) {
			best = i
			bestLevel = level
		}
	}
	ready := q.items[best]

	// 删除被选中元素。队列顺序本身不代表发送顺序，用末尾元素搬移即可，
	// 同时清理尾部位以避免指针滞留。
	last := len(q.items) - 1
	q.items[best] = q.items[last]
	q.items[last] = nil
	q.items = q.items[:last]

	q.served++
	q.mu.Unlock()

	// 帧已离开等待队列，归还一个空位信号量（与从 channel 接收等价）。
	q.releaseToken()
	return ready
}

// shutdown 标记调度器关闭并唤醒发送协程与所有等待者，可重复调用。
func (q *sendScheduler) shutdown() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// len 返回等待中的帧数量，主要用于测试与诊断。
func (q *sendScheduler) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// releaseToken 归还一个队列空位。令牌计数与等待帧数严格守恒，
// default 分支仅为防御性保护。
func (q *sendScheduler) releaseToken() {
	select {
	case q.tokens <- struct{}{}:
	default:
	}
}

// effectiveLevel 计算帧在当前调度时刻的有效等级：
//   - 会话级控制帧（stream 为 nil）固定为 priorityControl，始终最先发送；
//   - 数据帧/流控制帧以所属流的最新等级为基础（排队期间 SetPriority
//     调整的等级立即生效），再叠加等待老化：每经过 priorityAgingPeriod
//     次其他帧发送提升 1 级，最高到 priorityMax。
func (r *sendReady) effectiveLevel(served uint64) int {
	if r.stream == nil {
		return priorityControl
	}
	level := r.stream.Priority() + int((served-r.queuedAt)/priorityAgingPeriod)
	if level > priorityMax {
		level = priorityMax
	}
	return level
}
