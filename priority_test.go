// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatedConn 在普通连接之外增加了“写闸门”：block 之后所有 Write 阻塞，
// unblock 后恢复；同时按写出顺序记录数据帧所属的流 ID。
// 由于 sendLoop 是唯一的写协程，记录顺序就是实际调度顺序。
type gatedConn struct {
	io.ReadWriteCloser
	gate    sync.Mutex
	blocked atomic.Int32

	recMu     sync.Mutex
	dataOrder []uint32
}

func (c *gatedConn) Write(b []byte) (int, error) {
	if c.gate.TryLock() {
		// 闸门处于开启状态，立即释放并继续写。
		c.gate.Unlock()
	} else {
		// 闸门关闭：阻塞到 unblock。
		c.blocked.Add(1)
		c.gate.Lock()
		c.blocked.Add(-1)
		c.gate.Unlock()
	}

	if isDataHeader(b) {
		c.recMu.Lock()
		c.dataOrder = append(c.dataOrder, binary.BigEndian.Uint32(b[4:8]))
		c.recMu.Unlock()
	}
	return c.ReadWriteCloser.Write(b)
}

func (c *gatedConn) block() {
	c.gate.Lock()
}

func (c *gatedConn) unblock() {
	c.gate.Unlock()
}

func (c *gatedConn) blockedWrites() int {
	return int(c.blocked.Load())
}

func (c *gatedConn) dataFrameOrder() []uint32 {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	out := make([]uint32, len(c.dataOrder))
	copy(out, c.dataOrder)
	return out
}

// isDataHeader 判断一次 Write 是否恰好携带一个 yamux 数据帧头。
func isDataHeader(b []byte) bool {
	return len(b) == headerSize &&
		b[0] == protoVersion &&
		b[1] == typeData
}

// frameCounterConn 包装服务端连接，统计每个流收到的数据帧数量，
// 用于不依赖时序地验证低优先级流是否持续前进。
type frameCounterConn struct {
	io.ReadWriteCloser
	mu    sync.Mutex
	buf   []byte
	count map[uint32]int64
}

func newFrameCounterConn(inner io.ReadWriteCloser) *frameCounterConn {
	return &frameCounterConn{
		ReadWriteCloser: inner,
		count:           make(map[uint32]int64),
	}
}

func (c *frameCounterConn) Read(p []byte) (int, error) {
	n, err := c.ReadWriteCloser.Read(p)
	if n > 0 {
		c.mu.Lock()
		c.buf = append(c.buf, p[:n]...)
		c.parseFrames()
		c.mu.Unlock()
	}
	return n, err
}

func (c *frameCounterConn) parseFrames() {
	for len(c.buf) >= headerSize {
		length := binary.BigEndian.Uint32(c.buf[8:12])
		total := headerSize + int(length)
		if len(c.buf) < total {
			return
		}
		if c.buf[0] == protoVersion && c.buf[1] == typeData {
			id := binary.BigEndian.Uint32(c.buf[4:8])
			c.count[id]++
		}
		c.buf = c.buf[total:]
	}
}

func (c *frameCounterConn) frames(id uint32) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count[id]
}

// asyncWrites 用于在多个 goroutine 中发起流写并统一等待结束。
type asyncWrites struct {
	wg sync.WaitGroup
}

func (a *asyncWrites) write(st *Stream, payload []byte) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_, _ = st.Write(payload)
	}()
}

func (a *asyncWrites) wait(t testing.TB, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("等待数据帧写出超时")
	}
}

func priorityTestConf(keepAlive bool) *Config {
	var conf *Config
	if keepAlive {
		conf = testConf()
	} else {
		conf = testConfNoKeepAlive()
	}
	conf.EnablePriorityScheduling = true
	return conf
}

func gatedPrioritySessions(t testing.TB, gate *gatedConn, writeTimeout time.Duration) (*Session, *Session) {
	t.Helper()
	rawClient, rawServer := testConnPipe(t)
	gate.ReadWriteCloser = rawClient
	clientConf := priorityTestConf(false)
	serverConf := priorityTestConf(false)
	if writeTimeout > 0 {
		clientConf.ConnectionWriteTimeout = writeTimeout
		serverConf.ConnectionWriteTimeout = writeTimeout
	}
	return testClientServerConfig(t, gate, rawServer, clientConf, serverConf)
}

// openAcceptedStreams 打开 n 条流并在服务端全部 Accept（完成 SYN/ACK），
// 返回客户端与服务端两侧的流，顺序一一对应。
func openAcceptedStreams(t testing.TB, client, server *Session, n int) ([]*Stream, []*Stream) {
	t.Helper()
	clientStreams := make([]*Stream, n)
	serverStreams := make([]*Stream, n)
	for i := 0; i < n; i++ {
		cs, err := client.OpenStream()
		if err != nil {
			t.Fatalf("open stream %d: %v", i, err)
		}
		ss, err := server.AcceptStream()
		if err != nil {
			t.Fatalf("accept stream %d: %v", i, err)
		}
		clientStreams[i] = cs
		serverStreams[i] = ss
	}
	return clientStreams, serverStreams
}

// waitUntil 轮询条件直到成立或超时。
func waitUntil(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// pendingWrites 返回调度队列中等待与正在阻塞写出的帧总数。
func pendingWrites(s *Session, gate *gatedConn) int {
	n := 0
	if s.scheduler != nil {
		n = s.scheduler.len()
	}
	return n + gate.blockedWrites()
}

func equalUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPriorityConfig_DefaultOff(t *testing.T) {
	if DefaultConfig().EnablePriorityScheduling {
		t.Fatalf("EnablePriorityScheduling 必须默认关闭")
	}

	client, server := testClientServer(t)
	if client.scheduler != nil || server.scheduler != nil {
		t.Fatalf("未开启时不应创建优先级调度器")
	}
}

func TestStreamPriority_SetAndValidate(t *testing.T) {
	client, _ := testClientServer(t)
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	if p := stream.Priority(); p != 0 {
		t.Fatalf("新流默认等级应为 0，实际 %d", p)
	}

	if err := stream.SetPriority(7); err != nil {
		t.Fatalf("设置合法等级 7 失败: %v", err)
	}
	if p := stream.Priority(); p != 7 {
		t.Fatalf("等级应为 7，实际 %d", p)
	}

	// 非法等级返回错误且保持原值。
	if err := stream.SetPriority(-1); !errors.Is(err, ErrInvalidPriority) {
		t.Fatalf("期望 ErrInvalidPriority，实际 %v", err)
	}
	if err := stream.SetPriority(8); !errors.Is(err, ErrInvalidPriority) {
		t.Fatalf("期望 ErrInvalidPriority，实际 %v", err)
	}
	if p := stream.Priority(); p != 7 {
		t.Fatalf("非法等级后原等级应保持 7，实际 %d", p)
	}

	if err := stream.SetPriority(0); err != nil {
		t.Fatalf("设置合法等级 0 失败: %v", err)
	}
	if p := stream.Priority(); p != 0 {
		t.Fatalf("等级应为 0，实际 %d", p)
	}
}

func TestStreamPriority_ConcurrentAccess(t *testing.T) {
	client, _ := testClientServer(t)
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					if err := stream.SetPriority(rand.Intn(8)); err != nil {
						t.Errorf("合法等级设置失败: %v", err)
						return
					}
					_ = stream.Priority()
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(done)
	wg.Wait()
}

// TestSendScheduler_Ordering 直接对调度器做确定性验证：
// 控制帧优先、高等级优先、同级 FIFO、老化防饥饿、排队帧采用最新等级。
func TestSendScheduler_Ordering(t *testing.T) {
	newReady := func(t testing.TB, id uint32, priority int) *sendReady {
		st := newStream(nil, id, streamInit)
		if err := st.SetPriority(priority); err != nil {
			t.Fatal(err)
		}
		return &sendReady{
			Hdr:    header(make([]byte, headerSize)),
			stream: st,
			Err:    make(chan error, 1),
		}
	}
	newControl := func() *sendReady {
		return &sendReady{
			Hdr:    header(make([]byte, headerSize)),
			stream: nil,
			Err:    make(chan error, 1),
		}
	}
	// 单元测试中令牌缓冲始终充足，nil 的超时/关闭通道不会被选中。
	put := func(t testing.TB, q *sendScheduler, r *sendReady) {
		t.Helper()
		if err := q.put(r, nil, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("控制帧最高优先", func(t *testing.T) {
		q := newSendScheduler(sendQueueCapacity)
		put(t, q, newReady(t, 1, 0))
		put(t, q, newReady(t, 2, 7))
		put(t, q, newControl())
		if r := q.pop(); r.stream != nil {
			t.Fatalf("控制帧应最先发送，实际 streamID=%d", r.stream.id)
		}
		if r := q.pop(); r.stream.id != 2 {
			t.Fatalf("其次应为高等级流，实际 streamID=%d", r.stream.id)
		}
		if r := q.pop(); r.stream.id != 1 {
			t.Fatalf("最后应为低等级流，实际 streamID=%d", r.stream.id)
		}
	})

	t.Run("同等级按就绪先后", func(t *testing.T) {
		q := newSendScheduler(sendQueueCapacity)
		for _, id := range []uint32{10, 20, 30} {
			put(t, q, newReady(t, id, 3))
		}
		for _, want := range []uint32{10, 20, 30} {
			if r := q.pop(); r.stream.id != want {
				t.Fatalf("同等级应 FIFO，期望 %d 实际 %d", want, r.stream.id)
			}
		}
	})

	t.Run("高等级先于先就绪的低等级", func(t *testing.T) {
		q := newSendScheduler(sendQueueCapacity)
		put(t, q, newReady(t, 1, 0))
		put(t, q, newReady(t, 2, 7))
		put(t, q, newReady(t, 3, 3))
		for _, want := range []uint32{2, 3, 1} {
			if r := q.pop(); r.stream.id != want {
				t.Fatalf("期望顺序 2,3,1，实际出现 %d", r.stream.id)
			}
		}
	})

	t.Run("老化防止低等级饥饿", func(t *testing.T) {
		q := newSendScheduler(sendQueueCapacity)
		low := newReady(t, 99, 0)
		put(t, q, low)
		// 低等级帧等待期间，连续到达 28 个高等级帧。
		for i := uint32(200); i < 228; i++ {
			put(t, q, newReady(t, i, 7))
		}
		for i := 0; i < 28; i++ {
			if r := q.pop(); r.stream.id == low.stream.id {
				t.Fatalf("低等级帧不应在第 %d 次调度就插队", i+1)
			}
		}
		// 第 29 次调度时低等级帧已老化到最高等级，且就绪序号更早，
		// 必须获得发送机会。
		if r := q.pop(); r.stream.id != low.stream.id {
			t.Fatalf("老化后低等级帧必须被调度，实际 streamID=%d", r.stream.id)
		}
	})

	t.Run("排队帧采用最新等级", func(t *testing.T) {
		q := newSendScheduler(sendQueueCapacity)
		a := newReady(t, 1, 0)
		d := newReady(t, 2, 7)
		e := newReady(t, 3, 7)
		put(t, q, a)
		put(t, q, d)
		put(t, q, e)
		// 入队后动态调整：A 提到最高，D 降到最低。
		if err := a.stream.SetPriority(7); err != nil {
			t.Fatal(err)
		}
		if err := d.stream.SetPriority(0); err != nil {
			t.Fatal(err)
		}
		for _, want := range []uint32{1, 3, 2} {
			if r := q.pop(); r.stream.id != want {
				t.Fatalf("动态调整后期望顺序 1,3,2，实际出现 %d", r.stream.id)
			}
		}
	})
}

// TestPriorityScheduling_HighPriorityFirst 验证：闸门关闭时入队的帧，
// 正在写出的一帧可以完成，之后严格按高等级优先发送。
func TestPriorityScheduling_HighPriorityFirst(t *testing.T) {
	gate := &gatedConn{}
	client, server := gatedPrioritySessions(t, gate, 5*time.Second)

	clients, servers := openAcceptedStreams(t, client, server, 3)
	low, mid, high := clients[0], clients[1], clients[2]
	if err := low.SetPriority(0); err != nil {
		t.Fatal(err)
	}
	if err := mid.SetPriority(3); err != nil {
		t.Fatal(err)
	}
	if err := high.SetPriority(7); err != nil {
		t.Fatal(err)
	}

	// 建立后再关闭写闸门，排队 3 个数据帧（第 1 个成为正在写出的帧）。
	gate.block()
	var writes asyncWrites
	payload := bytes.Repeat([]byte{'x'}, 64)
	writes.write(low, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 1 }, "低等级帧入队")
	writes.write(high, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 2 }, "高等级帧入队")
	writes.write(mid, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 3 }, "中等级帧入队")

	gate.unblock()
	writes.wait(t, 2*time.Second)

	// 正在写出的低等级帧先完成，之后高(7)、中(3)、低(排队中的 0)。
	got := gate.dataFrameOrder()
	want := []uint32{low.StreamID(), high.StreamID(), mid.StreamID()}
	if !equalUint32(got, want) {
		t.Fatalf("发送顺序错误：got %v want %v", got, want)
	}

	// 字节与顺序在接收侧仍然正确。
	for i, ss := range servers {
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(ss, got); err != nil {
			t.Fatalf("读取流 %d: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("流 %d 数据内容错误", i)
		}
	}
}

// TestPriorityScheduling_SamePriorityFIFO 验证同等级严格按就绪先后。
func TestPriorityScheduling_SamePriorityFIFO(t *testing.T) {
	gate := &gatedConn{}
	client, server := gatedPrioritySessions(t, gate, 5*time.Second)
	clients, _ := openAcceptedStreams(t, client, server, 3)

	gate.block()
	var writes asyncWrites
	payload := bytes.Repeat([]byte{'f'}, 64)
	writes.write(clients[0], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 1 }, "帧 1 入队")
	writes.write(clients[1], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 2 }, "帧 2 入队")
	writes.write(clients[2], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 3 }, "帧 3 入队")
	gate.unblock()
	writes.wait(t, 2*time.Second)

	got := gate.dataFrameOrder()
	want := []uint32{clients[0].StreamID(), clients[1].StreamID(), clients[2].StreamID()}
	if !equalUint32(got, want) {
		t.Fatalf("同等级应 FIFO：got %v want %v", got, want)
	}
}

// TestPriorityScheduling_DynamicPriority 验证已排队但未发送的数据采用
// 最新等级：入队顺序 X(在写),A(0),D(7),E(7)，随后 A 升到 7、D 降到 0，
// 实际发送应为 X,A,E,D。
func TestPriorityScheduling_DynamicPriority(t *testing.T) {
	gate := &gatedConn{}
	client, server := gatedPrioritySessions(t, gate, 5*time.Second)
	clients, servers := openAcceptedStreams(t, client, server, 4)
	x, a, d, e := clients[0], clients[1], clients[2], clients[3]
	if err := a.SetPriority(0); err != nil {
		t.Fatal(err)
	}
	if err := d.SetPriority(7); err != nil {
		t.Fatal(err)
	}
	if err := e.SetPriority(7); err != nil {
		t.Fatal(err)
	}

	gate.block()
	var writes asyncWrites
	payload := bytes.Repeat([]byte{'d'}, 64)
	writes.write(x, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 1 }, "X 入队")
	writes.write(a, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 2 }, "A 入队")
	writes.write(d, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 3 }, "D 入队")
	writes.write(e, payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 4 }, "E 入队")

	// 排队期间动态调整等级。
	if err := a.SetPriority(7); err != nil {
		t.Fatal(err)
	}
	if err := d.SetPriority(0); err != nil {
		t.Fatal(err)
	}

	gate.unblock()
	writes.wait(t, 2*time.Second)

	got := gate.dataFrameOrder()
	want := []uint32{x.StreamID(), a.StreamID(), e.StreamID(), d.StreamID()}
	if !equalUint32(got, want) {
		t.Fatalf("动态调整后发送顺序错误：got %v want %v", got, want)
	}

	for i, ss := range servers {
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(ss, got); err != nil {
			t.Fatalf("读取流 %d: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("流 %d 数据损坏", i)
		}
	}
}

// TestPriorityScheduling_DataIntegrity 并发多流发送大块数据，期间持续
// 动态调整优先级，验证每条流收到的字节、顺序与长度完全正确。
func TestPriorityScheduling_DataIntegrity(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		name := "PriorityOn"
		if !enabled {
			name = "PriorityOff"
		}
		t.Run(name, func(t *testing.T) {
			rawClient, rawServer := testConnPipe(t)
			clientConf := testConfNoKeepAlive()
			serverConf := testConfNoKeepAlive()
			clientConf.EnablePriorityScheduling = enabled
			serverConf.EnablePriorityScheduling = enabled
			client, server := testClientServerConfig(t, rawClient, rawServer, clientConf, serverConf)

			const streamCount = 8
			const streamSize = 300 * 1024

			clients := make([]*Stream, streamCount)
			for i := 0; i < streamCount; i++ {
				st, err := client.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				if err := st.SetPriority(i % 8); err != nil {
					t.Fatal(err)
				}
				clients[i] = st
			}

			stopChurn := make(chan struct{})
			var churnWg sync.WaitGroup
			for i := 0; i < streamCount; i++ {
				st := clients[i]
				churnWg.Add(1)
				go func() {
					defer churnWg.Done()
					for {
						select {
						case <-stopChurn:
							return
						case <-time.After(2 * time.Millisecond):
							_ = st.SetPriority(rand.Intn(8))
						}
					}
				}()
			}

			// 接收侧：每流独立 goroutine 接收并校验全部数据。
			// 校验模式按 StreamID 绑定（Accept 顺序不要求与 Open 一致）。
			var serveWg sync.WaitGroup
			serveErr := make(chan error, streamCount)
			for i := 0; i < streamCount; i++ {
				serveWg.Add(1)
				go func() {
					defer serveWg.Done()
					st, err := server.AcceptStream()
					if err != nil {
						serveErr <- err
						return
					}
					data, err := io.ReadAll(st)
					if err != nil {
						serveErr <- err
						return
					}
					if len(data) != streamSize {
						serveErr <- errors.New("长度不符")
						return
					}
					id := int(st.StreamID())
					for j := range data {
						if data[j] != byte(j*31+id*17) {
							serveErr <- errors.New("内容不符")
							return
						}
					}
				}()
			}

			// 发送侧：小分片制造大量数据帧。
			var sendWg sync.WaitGroup
			sendErr := make(chan error, streamCount)
			for i := 0; i < streamCount; i++ {
				st := clients[i]
				sendWg.Add(1)
				go func() {
					defer sendWg.Done()
					id := int(st.StreamID())
					payload := make([]byte, streamSize)
					for j := range payload {
						payload[j] = byte(j*31 + id*17)
					}
					for off := 0; off < len(payload); off += 4096 {
						end := off + 4096
						if end > len(payload) {
							end = len(payload)
						}
						n, err := st.Write(payload[off:end])
						if err != nil {
							sendErr <- err
							return
						}
						if n != end-off {
							sendErr <- errors.New("短写")
							return
						}
					}
					if err := st.Close(); err != nil {
						sendErr <- err
					}
				}()
			}

			sendWg.Wait()
			close(stopChurn)
			churnWg.Wait()
			serveWg.Wait()

			select {
			case err := <-sendErr:
				t.Fatalf("发送错误: %v", err)
			default:
			}
			select {
			case err := <-serveErr:
				t.Fatalf("接收校验错误: %v", err)
			default:
			}
		})
	}
}

// TestPriorityScheduling_NoStarvation 验证持续繁忙的高等级流不会让
// 仍有发送窗口的低等级流长期停摆：多条高等级流持续形成发送积压时，
// 低等级流仍随老化机制持续增加已发送帧数，且最终全部完成。
func TestPriorityScheduling_NoStarvation(t *testing.T) {
	rawClient, rawServer := testConnPipe(t)
	counter := newFrameCounterConn(rawServer)
	client, server := testClientServerConfig(t, rawClient, counter,
		priorityTestConf(false), priorityTestConf(false))

	const highStreamCount = 8
	const chunkSize = 4096
	const highChunksPerStream = 400 // 聚合约 12.5MB，持续形成高等级积压
	const lowChunks = 120

	highs := make([]*Stream, highStreamCount)
	for i := 0; i < highStreamCount; i++ {
		st, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetPriority(7); err != nil {
			t.Fatal(err)
		}
		highs[i] = st
	}
	low, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	lowID := low.StreamID()

	// 服务端接收全部流并持续排空，保持发送窗口开放。
	for i := 0; i < highStreamCount+1; i++ {
		st, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		go func(st *Stream) { _, _ = io.Copy(io.Discard, st) }(st)
	}

	chunk := bytes.Repeat([]byte{'h'}, chunkSize)
	var wg sync.WaitGroup
	for i := 0; i < highStreamCount; i++ {
		st := highs[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < highChunksPerStream; j++ {
				if _, err := st.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < lowChunks; j++ {
			if _, err := low.Write(chunk); err != nil {
				return
			}
		}
	}()

	highFrames := func() int64 {
		var total int64
		for _, st := range highs {
			total += counter.frames(st.StreamID())
		}
		return total
	}

	// 在高等级聚合发送的三个水位，低等级流都必须有新进展。
	prevLow := int64(0)
	for _, mark := range []int64{200, 600, 1200} {
		waitUntil(t, 10*time.Second, func() bool {
			return highFrames() >= mark && counter.frames(lowID) > prevLow
		}, "高等级流达到发送水位且低等级流取得新进展")
		cur := counter.frames(lowID)
		t.Logf("高等级流聚合 >=%d 帧时，低等级流已发送 %d 帧", mark, cur)
		prevLow = cur
	}

	waitUntil(t, 15*time.Second, func() bool {
		return highFrames() >= highStreamCount*highChunksPerStream &&
			counter.frames(lowID) >= lowChunks
	}, "所有流全部发送完成")
	wg.Wait()

	// 高等级流总体应获得明显更多的调度机会。
	if got := counter.frames(lowID); got >= highFrames()/2 {
		t.Fatalf("优先级未生效：低等级 %d 帧，高等级聚合 %d 帧", got, highFrames())
	}
}

// TestPriorityScheduling_WriteTimeoutRecovery 验证写超时不会卡住
// 其他流：恢复发送后，新流数据与 Ping 都能成功。
func TestPriorityScheduling_WriteTimeoutRecovery(t *testing.T) {
	gate := &gatedConn{}
	client, server := gatedPrioritySessions(t, gate, 250*time.Millisecond)
	clients, servers := openAcceptedStreams(t, client, server, 3)

	// 接收侧仅排空超时残留的前两条流；第三条流留给后面的显式读取，
	// 避免同一条流出现两个读协程（Stream.Read 不可重入）。
	for _, ss := range servers[:2] {
		go func(st *Stream) { _, _ = io.Copy(io.Discard, st) }(ss)
	}

	gate.block()
	payload := bytes.Repeat([]byte{'t'}, 64)
	errCh := make(chan error, 2)
	go func() { _, err := clients[0].Write(payload); errCh <- err }()
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 1 }, "帧 1 排队/写出")
	go func() { _, err := clients[1].Write(payload); errCh <- err }()
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 2 }, "帧 2 排队")

	deadline := time.Now().Add(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrConnectionWriteTimeout) {
				t.Fatalf("期望写超时，实际 %v", err)
			}
		case <-time.After(time.Until(deadline)):
			t.Fatalf("写未在超时时间内返回")
		}
	}

	// 恢复底层连接：会话应自愈，控制消息与新数据均成功。
	gate.unblock()

	waitUntil(t, 3*time.Second, func() bool {
		_, err := client.Ping()
		return err == nil
	}, "恢复后 Ping 成功")

	if _, err := clients[2].Write(payload); err != nil {
		t.Fatalf("恢复后新流写入失败: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(servers[2], got); err != nil {
		t.Fatalf("恢复后读取新流数据失败: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("恢复后数据内容错误")
	}
}

// TestPriorityScheduling_StreamCloseNotStall 验证流关闭不影响其他流：
// 排队数据 + FIN 的流，对端先收数据再收 EOF；其他流数据完好；Ping 成功。
func TestPriorityScheduling_StreamCloseNotStall(t *testing.T) {
	gate := &gatedConn{}
	client, server := gatedPrioritySessions(t, gate, 5*time.Second)
	clients, servers := openAcceptedStreams(t, client, server, 4)

	gate.block()
	var writes asyncWrites
	payload := bytes.Repeat([]byte{'c'}, 64)
	writes.write(clients[0], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 1 }, "流1 入队")
	writes.write(clients[1], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 2 }, "流2 入队")
	writes.write(clients[2], payload)
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 3 }, "流3 入队")

	// 闸门关闭期间并发关闭“带排队数据的流 2”和“空闲流 4”：
	// FIN 帧入队但不阻塞测试；等待其确实排队后再统一开闸。
	closeErr := make(chan error, 2)
	go func() { closeErr <- clients[1].Close() }()
	go func() { closeErr <- clients[3].Close() }()
	waitUntil(t, 2*time.Second, func() bool { return pendingWrites(client, gate) == 5 }, "两个 FIN 入队")

	gate.unblock()
	writes.wait(t, 2*time.Second)
	for i := 0; i < 2; i++ {
		if err := <-closeErr; err != nil {
			t.Fatalf("关闭流失败: %v", err)
		}
	}

	if _, err := client.Ping(); err != nil {
		t.Fatalf("关闭流后 Ping 失败: %v", err)
	}

	// 流 1、流 3：数据完整。
	for _, idx := range []int{0, 2} {
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(servers[idx], got); err != nil {
			t.Fatalf("读取流 %d: %v", idx, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("流 %d 数据损坏", idx)
		}
	}

	// 流 2：排队数据送达后得到 EOF（FIN 不越过同流已排队数据）。
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(servers[1], got); err != nil {
		t.Fatalf("读取已关闭流的排队数据: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("已关闭流数据损坏")
	}
	if _, err := servers[1].Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("期望 EOF，实际 %v", err)
	}

	// 流 4：直接 EOF。
	if _, err := servers[3].Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("空闲关闭流期望 EOF，实际 %v", err)
	}
}

// TestPriorityScheduling_SessionCloseUnblocks 验证底层写阻塞且队列
// 被打满时关闭会话，所有阻塞的发送方都能及时解除阻塞。
func TestPriorityScheduling_SessionCloseUnblocks(t *testing.T) {
	rawClient, rawServer := testConnPipe(t)
	gate := &gatedConn{ReadWriteCloser: rawClient}
	clientConf := priorityTestConf(false)
	serverConf := priorityTestConf(false)
	clientConf.ConnectionWriteTimeout = 30 * time.Second
	serverConf.ConnectionWriteTimeout = 30 * time.Second
	client, server := testClientServerConfig(t, gate, rawServer, clientConf, serverConf)

	const n = sendQueueCapacity + 6
	clients, _ := openAcceptedStreams(t, client, server, n)

	// 会话关闭时必须释放写闸门：让卡在闸门上的发送协程继续走到
	// 已关闭的底层连接并报错退出（真实连接的 Close 可直接打断阻塞写）。
	go func() {
		<-client.CloseChan()
		gate.unblock()
	}()

	gate.block()
	payload := bytes.Repeat([]byte{'q'}, 64)
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = clients[idx].Write(payload)
		}(i)
	}

	// 等待队列被打满（64 排队 + 1 在写，其余在入队处阻塞）。
	waitUntil(t, 2*time.Second, func() bool {
		return pendingWrites(client, gate) >= sendQueueCapacity+1
	}, "发送队列打满")

	closeDone := make(chan struct{})
	go func() {
		_ = client.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("写阻塞时关闭会话卡住")
	}

	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Fatalf("流 %d 在会话关闭后应收到错误", i)
		}
	}

	// 关闭后再打开流应立即失败，而不是阻塞。
	done := make(chan error, 1)
	go func() { _, err := client.OpenStream(); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("关闭后打开流应失败")
		}
	case <-time.After(time.Second):
		t.Fatalf("关闭后 OpenStream 仍阻塞")
	}
}

// TestPriorityScheduling_PingDuringFlood 验证数据洪峰中控制消息
// （Ping、保活心跳）仍能及时完成。
func TestPriorityScheduling_PingDuringFlood(t *testing.T) {
	conf := priorityTestConf(true)
	conf.KeepAliveInterval = 100 * time.Millisecond
	logs := captureLogs(conf)
	serverConf := testConfNoKeepAlive()
	rawClient, rawServer := testConnPipe(t)
	client, server := testClientServerConfig(t, rawClient, rawServer, conf, serverConf)

	const floodStreams = 4
	clients, servers := openAcceptedStreams(t, client, server, floodStreams)

	// 服务端排空收到的数据，保持窗口开放。
	for _, st := range servers {
		go func(st *Stream) { _, _ = io.Copy(io.Discard, st) }(st)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	payload := bytes.Repeat([]byte{'p'}, 16*1024)
	for i := 0; i < floodStreams; i++ {
		st := clients[i]
		if err := st.SetPriority(i % 8); err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := st.Write(payload); err != nil {
					return
				}
			}
		}()
	}

	for i := 0; i < 5; i++ {
		rtt, err := client.Ping()
		if err != nil {
			t.Fatalf("洪峰中 Ping 失败: %v", err)
		}
		if rtt <= 0 || rtt > 2*time.Second {
			t.Fatalf("洪峰中 Ping 延迟异常: %v", rtt)
		}
		time.Sleep(60 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	// 经历多个保活周期后会话不应因心跳被饿死而报错关闭。
	if client.IsClosed() {
		t.Fatalf("会话在洪峰中被关闭: %s", logs.String())
	}
	if s := logs.String(); s != "" && s != "<nil>" {
		t.Fatalf("不应产生错误日志，实际: %q", s)
	}
}
