// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// 本文件覆盖可选会话级接收窗口（MaxSessionReceiveWindowSize）的行为：
// 配置校验、协商、慢读者多流总额度约束、读取/关闭唤醒、控制帧不被阻塞、
// 大写入分块完整性、写入截止时间、流/会话关闭清理、并发多流、会话间隔离，
// 以及与不支持该特性的旧版本对端的线路互通。所有用例均可重复执行，
// 并预期在 go test -race 下稳定通过。

// testWindow 返回启用会话级接收窗口、关闭 keepalive 的测试配置，
// 单流窗口保持初始的 256KB，便于精确推导总额度边界。
func testWindowConf(window uint32) *Config {
	conf := testConfNoKeepAlive()
	conf.MaxSessionReceiveWindowSize = window
	return conf
}

func testWindowPair(t testing.TB, window uint32) (*Session, *Session) {
	clientConn, serverConn := testConnTCP(t)
	conf := testWindowConf(window)
	return testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())
}

// waitPeerSessionWindowActive 等待本端收到对端的会话窗口通告并激活发送端
// 额度记账，使后续断言不再依赖启动时序。
func waitPeerSessionWindowActive(t testing.TB, s *Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		on, _, _ := s.peerSessionSnapshot()
		if on {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("peer session window never activated")
}

// pattern 生成可校验的确定性数据。
func pattern(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i + seed)
	}
	return b
}

func checkPattern(t testing.TB, b []byte, seed int) {
	t.Helper()
	for i := range b {
		if b[i] != byte(i+seed) {
			t.Fatalf("pattern mismatch at %d: got %d want %d", i, b[i], byte(i+seed))
		}
	}
}

// asyncWrite 在 goroutine 中写入完整数据，通过返回的 channel 回报结果。
func asyncWrite(s *Stream, b []byte) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := s.Write(b)
		ch <- err
	}()
	return ch
}

// assertBlocked 在宽限期内确认写入尚未完成（负面探针：若提前完成说明额度
// 约束失效）。
func assertBlocked(t testing.TB, ch <-chan error, what string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s should have blocked, returned err=%v", what, err)
	case <-time.After(75 * time.Millisecond):
	}
}

func assertDone(t testing.TB, ch <-chan error, timeout time.Duration, what string) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("%s failed: %v", what, err)
		}
	case <-time.After(timeout):
		t.Fatalf("%s timed out", what)
	}
}

// readFull 读取恰好 len(buf) 字节。
func readFull(t testing.TB, r io.Reader, buf []byte, what string) {
	t.Helper()
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("%s: read failed: %v", what, err)
	}
}

func TestSessionWindow_ConfigVerify(t *testing.T) {
	// 零值保持原有行为，合法。
	conf := testWindowConf(0)
	if err := VerifyConfig(conf); err != nil {
		t.Fatalf("zero value should be allowed: %v", err)
	}

	// 小于初始单流窗口的值被拒绝。
	conf = testWindowConf(initialStreamWindow - 1)
	if err := VerifyConfig(conf); err == nil {
		t.Fatalf("session window below initial stream window should be rejected")
	}

	// 等于初始单流窗口合法。
	conf = testWindowConf(initialStreamWindow)
	if err := VerifyConfig(conf); err != nil {
		t.Fatalf("session window of %d should be allowed: %v", initialStreamWindow, err)
	}
}

func TestSessionWindow_Negotiation(t *testing.T) {
	// 默认配置（零值）：双方都不应激活会话流控，也不应通告。
	c1, s1 := testClientServer(t)
	if _, err := c1.Ping(); err != nil {
		t.Fatal(err)
	}
	if on, _, _ := c1.peerSessionSnapshot(); on {
		t.Fatal("client should not activate peer session window by default")
	}
	if on, _, _ := s1.peerSessionSnapshot(); on {
		t.Fatal("server should not activate peer session window by default")
	}
	if c1.sessionRecvOn || s1.sessionRecvOn {
		t.Fatal("session receive window should be disabled by default")
	}
	_ = c1.Close()
	_ = s1.Close()

	// 双方启用：通告互达后两侧发送端记账均激活。
	c2, s2 := testWindowPair(t, initialStreamWindow)
	waitPeerSessionWindowActive(t, c2)
	waitPeerSessionWindowActive(t, s2)
	if !c2.sessionRecvOn || !s2.sessionRecvOn {
		t.Fatal("session receive window should be enabled")
	}
}

// TestSessionWindow_CapacityBoundsUnread 验证慢读者场景下，多条流的未读
// 数据总量受接收端会话额度约束：窗口耗尽后写入方全部阻塞，不能继续累加。
func TestSessionWindow_CapacityBoundsUnread(t *testing.T) {
	const window = initialStreamWindow // 256KB
	client, server := testWindowPair(t, window)

	const n = 4
	clientStreams := make([]*Stream, n)
	serverStreams := make([]*Stream, n)
	for i := 0; i < n; i++ {
		cs, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		ss, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		clientStreams[i] = cs
		serverStreams[i] = ss
	}
	waitPeerSessionWindowActive(t, client)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// 每条流都尝试写满自己的单流窗口（256KB）；阻塞中的流最终在
			// Cleanup 关闭会话时返回错误，忽略即可。
			_, _ = clientStreams[i].Write(pattern(i+1, int(initialStreamWindow)))
		}(i)
	}
	close(start)

	// 等待发送侧静止（计数器连续稳定）。
	checkWritten := func() int64 {
		// 通过接收端各流缓冲之和观测“未读数据总量”。
		var buffered int64
		for _, ss := range serverStreams {
			ss.recvLock.Lock()
			if ss.recvBuf != nil {
				buffered += int64(ss.recvBuf.Len())
			}
			ss.recvLock.Unlock()
		}
		return buffered
	}

	last := int64(-1)
	stable := 0
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cur := checkWritten()
		if cur == last {
			stable++
			if stable >= 3 {
				break
			}
		} else {
			stable = 0
			last = cur
		}
		time.Sleep(20 * time.Millisecond)
	}
	if buffered := checkWritten(); buffered > int64(window) {
		t.Fatalf("total unread data %d exceeds session window %d", buffered, window)
	}
	if buffered := checkWritten(); buffered == 0 {
		t.Fatal("expected data to be buffered before blocking")
	}
	t.Logf("buffered while readers idle: %d (window=%d)", checkWritten(), window)

	// 接收端开始慢速读取：每条流读一点，被阻塞的写入应随之继续，
	// 未读总量始终不超过窗口。
	stop := make(chan struct{})
	go func() {
		chunk := make([]byte, 4096)
		offsets := make([]int, n)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i, ss := range serverStreams {
				nn, err := ss.Read(chunk)
				if err != nil {
					return
				}
				for j := 0; j < nn; j++ {
					if chunk[j] != byte(offsets[i]+j+i+1) {
						t.Errorf("pattern mismatch stream=%d off=%d", i, offsets[i]+j)
						return
					}
				}
				offsets[i] += nn
			}
			if buffered := checkWritten(); buffered > int64(window) {
				t.Errorf("total unread data %d exceeds window %d during reading", buffered, window)
				return
			}
		}
	}()

	// 给慢速读取一段时间，确认期间持续有进展且不超额。
	time.Sleep(300 * time.Millisecond)
	close(stop)

	// 显式关闭会话以解除阻塞中的写入 goroutine，并等待它们退出
	// （Cleanup 中的再次 Close 是空操作）。
	_ = client.Close()
	_ = server.Close()
	wg.Wait()
}

// TestSessionWindow_BlockAndResumeOnRead 验证额度耗尽后写入阻塞，接收端从
// 任意流读走少量数据即可唤醒写入；流不会被重置，数据字节完整。
func TestSessionWindow_BlockAndResumeOnRead(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	open := func() (*Stream, *Stream) {
		cs, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		ss, err := server.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		return cs, ss
	}
	cs1, ss1 := open()
	cs2, ss2 := open()
	cs3, ss3 := open()
	waitPeerSessionWindowActive(t, client)

	// 两条流合计恰好填满会话窗口。
	assertDone(t, asyncWrite(cs1, pattern(1, 192*1024)), time.Second, "write stream1")
	assertDone(t, asyncWrite(cs2, pattern(2, 64*1024)), time.Second, "write stream2")

	// 第三条流的写入必须阻塞（窗口为 0，小写入也立即返还才能完成）。
	const grant = 16 * 1024
	w3 := asyncWrite(cs3, pattern(3, grant))
	assertBlocked(t, w3, "stream3 write")

	// 从流 1 读走 16KB：额度曾耗尽，接收端必须立即（不受半窗口批量阈值
	// 限制）返还，恰好唤醒流 3 的 16KB 写入。
	buf := make([]byte, grant)
	readFull(t, ss1, buf, "stream1 read")
	checkPattern(t, buf, 1)
	assertDone(t, w3, 2*time.Second, "stream3 write after read on stream1")

	// 流 3 再多写会再次阻塞，直到继续读取。
	w3b := asyncWrite(cs3, pattern(3+grant, grant))
	assertBlocked(t, w3b, "stream3 second write")
	readFull(t, ss2, buf, "stream2 read")
	checkPattern(t, buf, 2)
	assertDone(t, w3b, 2*time.Second, "stream3 second write after read stream2")

	// 校验流 3 收到的数据完整。
	got := make([]byte, 2*grant)
	readFull(t, ss3, got[:grant], "stream3 read 1")
	checkPattern(t, got[:grant], 3)
	readFull(t, ss3, got[grant:], "stream3 read 2")
	checkPattern(t, got[grant:], 3+grant)
}

// TestSessionWindow_ResumeOnGracefulClose 验证含未读数据的流被双向 FIN
// 完全关闭后，其占用的会话额度立即返还，其他流上阻塞的写入可以继续。
func TestSessionWindow_ResumeOnGracefulClose(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	cs1, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss1, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	cs2, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss2, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	cs3, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss3, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	const fill1 = 192 * 1024
	const fill2 = 64 * 1024
	assertDone(t, asyncWrite(cs1, pattern(11, fill1)), time.Second, "stream1 fill")
	assertDone(t, asyncWrite(cs2, pattern(22, fill2)), time.Second, "stream2 fill")

	w3 := asyncWrite(cs3, pattern(33, 128*1024))
	assertBlocked(t, w3, "stream3 write")

	// 客户端半关闭流 1（FIN），服务端此前完全没有读取流 1 的数据；
	// 服务端再关闭自己一侧：流被完全关闭，192KB 未读额度应被返还。
	if err := cs1.Close(); err != nil {
		t.Fatal(err)
	}
	// 等待服务端收到 FIN。
	deadline := time.Now().Add(2 * time.Second)
	for {
		ss1.stateLock.Lock()
		st := ss1.state
		ss1.stateLock.Unlock()
		if st == streamRemoteClose || st == streamClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FIN not received")
		}
		time.Sleep(time.Millisecond)
	}
	if err := ss1.Close(); err != nil {
		t.Fatal(err)
	}

	// 流 3 应获得额度并完成；流 1 未读数据被丢弃但流本身没有被重置
	// （是正常的 FIN 关闭路径）。
	assertDone(t, w3, 2*time.Second, "stream3 write after stream1 fully closed")

	got := make([]byte, 128*1024)
	readFull(t, ss3, got, "stream3 read")
	checkPattern(t, got, 33)

	// 流 2 数据完整可读。
	got2 := make([]byte, fill2)
	readFull(t, ss2, got2, "stream2 read")
	checkPattern(t, got2, 22)
}

// TestSessionWindow_ResumeOnResetClose 验证 StreamCloseTimeout 触发的
// RST 强制关闭同样会释放未读数据占用的会话额度，其他流写入不被卡死。
func TestSessionWindow_ResumeOnResetClose(t *testing.T) {
	const window = initialStreamWindow
	clientConn, serverConn := testConnTCP(t)
	clientConf := testWindowConf(window)
	serverConf := testWindowConf(window)
	serverConf.StreamCloseTimeout = 40 * time.Millisecond
	client, server := testClientServerConfig(t, clientConn, serverConn, clientConf, serverConf)

	cs1, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss1, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	cs2, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.AcceptStream(); err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	// 流 1 写入 192KB 后保持打开（不发 FIN），服务端不读，直接关闭：
	// 40ms 后强制 RST 清理，释放 192KB 额度。
	assertDone(t, asyncWrite(cs1, pattern(44, 192*1024)), time.Second, "stream1 fill")
	assertDone(t, asyncWrite(cs2, pattern(55, 64*1024)), time.Second, "stream2 fill")

	w3stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss3, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	w3 := asyncWrite(w3stream, pattern(66, 128*1024))
	assertBlocked(t, w3, "stream3 write")

	if err := ss1.Close(); err != nil {
		t.Fatal(err)
	}

	// RST 清理后流 3 写入必须恢复。
	assertDone(t, w3, 3*time.Second, "stream3 write after reset close")
	got := make([]byte, 128*1024)
	readFull(t, ss3, got, "stream3 read")
	checkPattern(t, got, 66)

	// 流 1 客户端最终观察到关闭/重置（非永久悬挂）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cs1.stateLock.Lock()
		st := cs1.state
		cs1.stateLock.Unlock()
		if st == streamReset || st == streamClosed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("stream1 client side should observe reset/close")
}

// TestSessionWindow_PingNotBlocked 验证额度耗尽、数据写入阻塞期间，
// Ping 仍能及时往返；窗口更新等控制帧也不被等待中的数据阻塞。
func TestSessionWindow_PingNotBlocked(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	// 写满窗口后继续写，使数据写入长期阻塞。
	assertDone(t, asyncWrite(cs, pattern(1, int(window))), time.Second, "fill")
	blocked := asyncWrite(cs, pattern(2, 4096))
	assertBlocked(t, blocked, "extra write")

	// 阻塞期间双向 Ping 都必须快速成功。
	for i := 0; i < 3; i++ {
		if _, err := client.Ping(); err != nil {
			t.Fatalf("client ping while blocked: %v", err)
		}
		if _, err := server.Ping(); err != nil {
			t.Fatalf("server ping while blocked: %v", err)
		}
	}

	// 读取放行后数据完整。
	got := make([]byte, int(window)+4096)
	readFull(t, ss, got[:window], "drain")
	checkPattern(t, got[:window], 1)
	assertDone(t, blocked, 2*time.Second, "blocked write after drain")
	readFull(t, ss, got[window:], "tail")
	checkPattern(t, got[window:], 2)
}

// TestSessionWindow_LargeWriteChunked 验证大写入随额度返还逐块完成，
// 最终字节完整、顺序正确。
func TestSessionWindow_LargeWriteChunked(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	const total = int(3*window + 12345)
	w := asyncWrite(cs, pattern(7, total))

	// 接收端用小缓冲区逐块读取并校验。
	got := make([]byte, total)
	chunk := make([]byte, 3333)
	var off int
	for off < total {
		want := len(chunk)
		if total-off < want {
			want = total - off
		}
		n, err := io.ReadFull(ss, chunk[:want])
		if err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
		copy(got[off:], chunk[:n])
		off += n
	}
	checkPattern(t, got, 7)
	assertDone(t, w, 2*time.Second, "large write completion")

	// 关闭后读到 EOF。
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// TestSessionWindow_WriteDeadline 验证写入截止时间：超时返回 ErrTimeout，
// 超时写入不会产生迟到数据，也不永久占用额度；超时后传输仍可正常完成。
func TestSessionWindow_WriteDeadline(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	cs, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	fill := pattern(1, int(window))
	assertDone(t, asyncWrite(cs, fill), time.Second, "fill")

	// 设置一个很短的写截止时间，阻塞写入必须超时返回。
	if err := cs.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, werr := cs.Write(make([]byte, 4096))
	if werr != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", werr)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("timeout took too long: %v", d)
	}

	// 清除截止时间；接收端排空窗口后，后续写入必须完整完成（证明超时没有
	// 永久占用额度、没有卡住的写入）。
	if err := cs.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, window)
	readFull(t, ss, got, "drain fill")
	checkPattern(t, got, 1)

	more := pattern(99, int(window+777))
	w := asyncWrite(cs, more)

	gotMore := make([]byte, len(more))
	readFull(t, ss, gotMore, "drain more")
	checkPattern(t, gotMore, 99)
	assertDone(t, w, 2*time.Second, "write after deadline")

	// 流中收到的全部数据必须恰好等于两次成功写入的拼接：超时写入既没有
	// 部分发出，也没有迟到数据。
	if _, err := cs.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	readFull(t, ss, one, "probe")
	if one[0] != 1 {
		t.Fatalf("unexpected trailing byte: %d", one[0])
	}
}

// TestSessionWindow_StreamCloseUnblocksWriter 验证流关闭时阻塞在会话额度
// 上的写入立即返回且不泄漏额度，其他流传输不受影响。
func TestSessionWindow_StreamCloseUnblocksWriter(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	cs1, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss1, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	cs2, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ss2, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, client)

	assertDone(t, asyncWrite(cs1, pattern(1, int(window))), time.Second, "fill stream1")
	blocked := asyncWrite(cs1, make([]byte, 4096))
	assertBlocked(t, blocked, "stream1 extra write")

	// 流 2 此刻同样拿不到额度，启动一个被阻塞的写入。
	w2 := asyncWrite(cs2, pattern(2, 100*1024))
	assertBlocked(t, w2, "stream2 write")

	// 关闭流 1（双向）：流 1 自己的阻塞写入必须立即失败，同时流 1 中未读
	// 的整窗数据被丢弃并返还会话额度，流 2 的写入随后可以完成。
	if err := cs1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ss1.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("blocked write should fail after stream close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked write did not return after stream close")
	}

	got := make([]byte, 100*1024)
	readFull(t, ss2, got, "stream2 read")
	checkPattern(t, got, 2)
	assertDone(t, w2, 2*time.Second, "stream2 write after stream1 close")

	if _, err := client.Ping(); err != nil {
		t.Fatalf("ping after close: %v", err)
	}
}

// TestSessionWindow_SessionCloseUnblocksWriters 验证会话关闭后所有阻塞在
// 额度上的写入都能及时返回，不留下卡住的 goroutine。
func TestSessionWindow_SessionCloseUnblocksWriters(t *testing.T) {
	const window = initialStreamWindow
	client, server := testWindowPair(t, window)

	const n = 5
	writers := make([]<-chan error, n)
	clientStreams := make([]*Stream, n)
	for i := 0; i < n; i++ {
		cs, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.AcceptStream(); err != nil {
			t.Fatal(err)
		}
		clientStreams[i] = cs
	}
	waitPeerSessionWindowActive(t, client)

	// 第一条流填满窗口，其余流全部阻塞。
	assertDone(t, asyncWrite(clientStreams[0], pattern(1, int(window))), time.Second, "fill")
	for i := 1; i < n; i++ {
		writers[i] = asyncWrite(clientStreams[i], pattern(i+1, 4096))
	}
	for i := 1; i < n; i++ {
		assertBlocked(t, writers[i], fmt.Sprintf("stream%d write", i+1))
	}

	// 关闭会话（底层连接关闭），所有写入必须返回错误。
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < n; i++ {
		select {
		case <-writers[i]:
		case <-time.After(3 * time.Second):
			t.Fatalf("stream%d writer stuck after session close", i+1)
		}
	}
}

// writeSeeded 在流首字节写入种子（任意非零值），随后写入确定性载荷。
// 并发 OpenStream 时流 ID 与 Accept 顺序可能交错，因此校验种子必须随流内
// 数据自身携带，不能依赖“第 i 个打开的流”这类外部配对。
func writeSeeded(t testing.TB, w io.Writer, seed, payloadLen int) {
	t.Helper()
	if _, err := w.Write([]byte{byte(seed)}); err != nil {
		t.Errorf("write seed: %v", err)
		return
	}
	if _, err := w.Write(pattern(seed, payloadLen)); err != nil {
		t.Errorf("write payload: %v", err)
	}
}

// readSeeded 读取单条流的“种子字节 + 确定性载荷”并逐字节校验，chunkSize
// 模拟慢读者的小粒度读取。
func readSeeded(t testing.TB, r io.Reader, payloadLen, chunkSize int) {
	t.Helper()
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Errorf("read seed: %v", err)
		return
	}
	seed := int(hdr[0])
	buf := make([]byte, chunkSize)
	off := 0
	for off < payloadLen {
		nn, err := r.Read(buf)
		if err != nil {
			t.Errorf("read payload seed=%d off=%d: %v", seed, off, err)
			return
		}
		for j := 0; j < nn; j++ {
			if buf[j] != byte(off+j+seed) {
				t.Errorf("mismatch seed=%d off=%d got=%d want=%d", seed, off+j, buf[j], byte(off+j+seed))
				return
			}
		}
		off += nn
	}
}

// TestSessionWindow_ConcurrentStreams 并发多流双向传输 + 慢读者，验证
// 总量受控、数据完整、无死锁；专为 -race 设计。
func TestSessionWindow_ConcurrentStreams(t *testing.T) {
	const window = 2 * initialStreamWindow // 512KB
	client, server := testWindowPair(t, window)

	const streams = 10
	const perStream = 300 * 1024
	var wg sync.WaitGroup

	// 用独立的失败收集器：任何 goroutine 失败都记录一次，t.Errorf 也会让
	// 用例最终失败（避免共享 channel 容量与关闭时序问题）。
	var failMu sync.Mutex
	failures := 0
	fail := func(err error) {
		failMu.Lock()
		failures++
		failMu.Unlock()
		t.Log(err)
	}
	subtestDone := make(chan struct{})

	for i := 0; i < streams; i++ {
		seed := i + 1
		wg.Add(2)

		// client -> server
		go func() {
			defer wg.Done()
			cs, err := client.OpenStream()
			if err != nil {
				fail(err)
				return
			}
			defer cs.Close()
			writeSeeded(t, cs, seed, perStream)
		}()

		go func() {
			defer wg.Done()
			ss, err := server.AcceptStream()
			if err != nil {
				fail(err)
				return
			}
			defer ss.Close()
			readSeeded(t, ss, perStream, 7000)
		}()
	}

	// 同时做 server -> client 的反向并发流。
	for i := 0; i < streams/2; i++ {
		seed := 100 + i
		wg.Add(2)
		go func() {
			defer wg.Done()
			ss, err := server.OpenStream()
			if err != nil {
				fail(err)
				return
			}
			defer ss.Close()
			writeSeeded(t, ss, seed, perStream)
		}()
		go func() {
			defer wg.Done()
			cs, err := client.AcceptStream()
			if err != nil {
				fail(err)
				return
			}
			defer cs.Close()
			readSeeded(t, cs, perStream, 12000)
		}()
	}

	go func() { wg.Wait(); close(subtestDone) }()
	select {
	case <-subtestDone:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent streams deadlocked")
	}
	if failures != 0 {
		t.Fatalf("concurrent transfer had %d failures", failures)
	}
}

// TestSessionWindow_Independence 验证不同会话的会话窗口互不影响：
// 一条会话因慢读者阻塞时，另一条会话仍可满速传输。
func TestSessionWindow_Independence(t *testing.T) {
	// 会话 A：256KB 小窗口，制造慢读者阻塞。
	clientA, serverA := testWindowPair(t, initialStreamWindow)
	// 会话 B：大窗口，独立连接。
	clientB, serverB := testWindowPair(t, 8*initialStreamWindow)

	csA, err := clientA.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serverA.AcceptStream(); err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, clientA)
	assertDone(t, asyncWrite(csA, pattern(1, int(initialStreamWindow))), time.Second, "A fill")
	blockedA := asyncWrite(csA, make([]byte, 4096))
	assertBlocked(t, blockedA, "session A blocked write")

	// 会话 B 上的大传输必须不受会话 A 阻塞影响而快速完成。
	csB, err := clientB.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	ssB, err := serverB.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	waitPeerSessionWindowActive(t, clientB)
	waitPeerSessionWindowActive(t, serverB)

	const sizeB = int(4 * initialStreamWindow)
	wB := asyncWrite(csB, pattern(5, sizeB))
	gotB := make([]byte, sizeB)
	readFull(t, ssB, gotB, "session B read")
	checkPattern(t, gotB, 5)
	assertDone(t, wB, 5*time.Second, "session B transfer")

	// 会话 A 依旧处于阻塞状态（没有被会话 B 的活动意外唤醒）。
	assertBlocked(t, blockedA, "session A should stay blocked")
}

// TestSessionWindow_OneSidedEnable 验证仅一端启用时与“旧行为”互通：
// 未启用侧不通告，启用侧向其发送数据时不做会话级阻塞，双向大数据传输
// 都能依靠单流窗口完成。
func TestSessionWindow_OneSidedEnable(t *testing.T) {
	for _, enabledIsClient := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled_client", false: "enabled_server"}[enabledIsClient], func(t *testing.T) {
			enabled := testWindowConf(initialStreamWindow)
			disabled := testWindowConf(0)
			var clientConf, serverConf *Config
			if enabledIsClient {
				clientConf, serverConf = enabled, disabled
			} else {
				clientConf, serverConf = disabled, enabled
			}
			clientConn, serverConn := testConnTCP(t)
			client, server := testClientServerConfig(t, clientConn, serverConn, clientConf, serverConf)

			// 未启用侧永远不激活发送端会话额度（对端没通告）。
			disabledSession, enabledSession := server, client
			if enabledIsClient {
				disabledSession, enabledSession = client, server
			}
			waitPeerSessionWindowActive(t, enabledSession)
			if on, _, _ := disabledSession.peerSessionSnapshot(); on {
				t.Fatal("disabled side must not activate session flow control")
			}

			// 双向大传输（远超 256KB），慢接收方按单流窗口节奏读取即可完成。
			for _, dir := range []string{"c2s", "s2c"} {
				var sender, receiver *Session
				if dir == "c2s" {
					sender, receiver = client, server
				} else {
					sender, receiver = server, client
				}
				cs, err := sender.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				ss, err := receiver.AcceptStream()
				if err != nil {
					t.Fatal(err)
				}
				const size = int(2 * initialStreamWindow)
				w := asyncWrite(cs, pattern(3, size))
				got := make([]byte, size)
				if _, err := io.ReadFull(ss, got); err != nil {
					t.Fatal(err)
				}
				checkPattern(t, got, 3)
				assertDone(t, w, 5*time.Second, dir+" transfer")
				_ = cs.Close()
				_ = ss.Close()
			}

			// Ping 正常。
			if _, err := client.Ping(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ---- 旧版本对端的最小线路模拟（不支持会话级窗口） ----

type rawHdr [headerSize]byte

func (h *rawHdr) encode(typ uint8, flags uint16, streamID, length uint32) {
	h[0] = protoVersion
	h[1] = typ
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint32(h[4:8], streamID)
	binary.BigEndian.PutUint32(h[8:12], length)
}

func readRawHdr(r io.Reader) (typ uint8, flags uint16, streamID, length uint32, err error) {
	var h rawHdr
	if _, err = io.ReadFull(r, h[:]); err != nil {
		return 0, 0, 0, 0, err
	}
	if h[0] != protoVersion {
		return 0, 0, 0, 0, ErrInvalidVersion
	}
	return h[1], binary.BigEndian.Uint16(h[2:4]),
		binary.BigEndian.Uint32(h[4:8]), binary.BigEndian.Uint32(h[8:12]), nil
}

// oldPeer 模拟不支持会话级窗口的旧版 yamux 对端：忽略所有 StreamID=0 的
// WindowUpdate，只实现单流窗口流控、Ping 应答、FIN/RST。
type oldPeer struct {
	t      testing.TB
	conn   io.ReadWriteCloser
	cli    bool
	wmu    sync.Mutex
	mu     sync.Mutex
	next   uint32
	lines  map[uint32]*oldLine
	zero   bool // 是否观察到 StreamID=0 的 SYN（永远不应发生）
	closed chan struct{}
}

type oldLine struct {
	id       uint32
	accepted bool
	mu       sync.Mutex
	cond     *sync.Cond
	buf      bytes.Buffer
	fin      bool
	rst      bool
	swin     int64 // 对端允许本端发送的单流窗口
}

func newOldLine() *oldLine {
	l := &oldLine{swin: int64(initialStreamWindow)}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func newOldPeer(t testing.TB, conn io.ReadWriteCloser, client bool) *oldPeer {
	p := &oldPeer{
		t:      t,
		conn:   conn,
		cli:    client,
		next:   2,
		lines:  map[uint32]*oldLine{},
		closed: make(chan struct{}),
	}
	if client {
		p.next = 1
	}
	go p.recvLoop()
	return p
}

func (p *oldPeer) sendRaw(typ uint8, flags uint16, id, length uint32, body []byte) error {
	var h rawHdr
	h.encode(typ, flags, id, length)
	p.wmu.Lock()
	defer p.wmu.Unlock()
	if _, err := p.conn.Write(h[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := p.conn.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func (p *oldPeer) recvLoop() {
	defer close(p.closed)
	for {
		typ, flags, id, length, err := readRawHdr(p.conn)
		if err != nil {
			return
		}
		switch typ {
		case typePing:
			if flags&flagSYN != 0 {
				if err := p.sendRaw(typePing, flagACK, 0, length, nil); err != nil {
					return
				}
			}
			continue
		case typeGoAway:
			return
		}

		// 旧版行为：StreamID=0 的 WindowUpdate 是无法识别的帧，直接忽略，
		// 绝不建流、绝不报错。
		if id == 0 {
			if typ == typeWindowUpdate {
				continue
			}
		}

		p.mu.Lock()
		line := p.lines[id]
		if flags&flagSYN != 0 {
			if id == 0 {
				p.zero = true
			}
			if line == nil {
				line = newOldLine()
				p.lines[id] = line
			}
		}
		p.mu.Unlock()

		switch typ {
		case typeData:
			body := make([]byte, length)
			if _, err := io.ReadFull(p.conn, body); err != nil {
				return
			}
			if line == nil {
				continue
			}
			line.mu.Lock()
			line.buf.Write(body)
			line.cond.Broadcast()
			line.mu.Unlock()
		case typeWindowUpdate:
			if line == nil {
				continue
			}
			line.mu.Lock()
			if flags&flagRST != 0 {
				line.rst = true
				line.cond.Broadcast()
				line.mu.Unlock()
				continue
			}
			if flags&flagFIN != 0 {
				line.fin = true
				line.cond.Broadcast()
			}
			if length > 0 {
				line.swin += int64(length)
				line.cond.Broadcast()
			}
			line.mu.Unlock()
		}
	}
}

func (p *oldPeer) accept() (uint32, *oldLine) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for id, l := range p.lines {
			if l.accepted {
				continue // 已接受过的流不重复返回
			}
			l.accepted = true
			l.id = id
			p.mu.Unlock()
			// 旧版接收端接受流时回复 ACK；map 条目必须保留，后续数据帧
			// 还要靠它定位接收状态。
			if err := p.sendRaw(typeWindowUpdate, flagACK, id, 0, nil); err != nil {
				p.t.Fatal(err)
			}
			return id, l
		}
		p.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	p.t.Fatal("old peer: no incoming stream")
	return 0, nil
}

// oldRead 从旧版模拟端读取数据，并按旧版语义每读 n 字节返还 n 字节单流窗口。
func (p *oldPeer) oldRead(line *oldLine, buf []byte) (int, error) {
	line.mu.Lock()
	for line.buf.Len() == 0 && !line.fin && !line.rst {
		line.cond.Wait()
	}
	if line.rst {
		line.mu.Unlock()
		return 0, ErrConnectionReset
	}
	if line.buf.Len() == 0 && line.fin {
		line.mu.Unlock()
		return 0, io.EOF
	}
	n, _ := line.buf.Read(buf)
	id := line.id
	line.mu.Unlock()
	if n > 0 {
		if err := p.sendRaw(typeWindowUpdate, 0, id, uint32(n), nil); err != nil {
			return n, err
		}
	}
	return n, nil
}

// open 让旧版模拟端打开一条流（仅受单流窗口约束，因为它从不解析会话窗口帧）。
func (p *oldPeer) open() (uint32, *oldLine) {
	p.mu.Lock()
	id := p.next
	p.next += 2
	line := newOldLine()
	line.id = id
	p.lines[id] = line
	p.mu.Unlock()
	if err := p.sendRaw(typeWindowUpdate, flagSYN, id, 0, nil); err != nil {
		p.t.Fatal(err)
	}
	return id, line
}

func (p *oldPeer) write(line *oldLine, b []byte) (int, error) {
	total := 0
	for total < len(b) {
		line.mu.Lock()
		for line.swin <= 0 && !line.rst {
			line.cond.Wait()
		}
		if line.rst {
			line.mu.Unlock()
			return total, ErrConnectionReset
		}
		n := len(b) - total
		if int64(n) > line.swin {
			n = int(line.swin)
		}
		line.swin -= int64(n)
		line.mu.Unlock()

		if err := p.sendRaw(typeData, 0, line.id, uint32(n), b[total:total+n]); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (p *oldPeer) closeWrite(line *oldLine) {
	_ = p.sendRaw(typeWindowUpdate, flagFIN, line.id, 0, nil)
}

func (p *oldPeer) close() { _ = p.conn.Close() }

// TestSessionWindow_InteropOldPeer 验证新版端点与不支持会话窗口的旧版
// 端点在真实线路上互通：
//   - 旧端发送、新版接收（接收端启用）：旧端无视通告/会话更新帧，仅靠单流
//     窗口即可完成大传输，且通告帧不会在旧端创建出 StreamID=0 的流；
//   - 新版发送、旧端接收：旧端不发通告，新版发送端不激活会话额度，按单流
//     窗口完成大传输；新版发往旧端的会话更新帧被旧端安全忽略；
//   - Ping 往返正常。
func TestSessionWindow_InteropOldPeer(t *testing.T) {
	t.Run("old_sends_new_receives", func(t *testing.T) {
		conf := testWindowConf(initialStreamWindow)
		_ = captureLogs(conf) // 忽略“missing stream 0”类告警噪声
		c1, c2 := testConnPipe(t)
		newSess, err := Client(c1, conf)
		if err != nil {
			t.Fatal(err)
		}
		old := newOldPeer(t, c2, false)
		defer old.close()
		defer newSess.Close()
		// 旧端不发通告：等待一个 Ping 往返（旧端会应答）足以确认线路就绪，
		// 同时新端的发送端会话额度必须保持未激活。
		if _, err := newSess.Ping(); err != nil {
			t.Fatal(err)
		}
		if on, _, _ := newSess.peerSessionSnapshot(); on {
			t.Fatal("session window must stay inactive against a peer that never announces")
		}

		id, line := old.open()
		_ = id
		// 新版侧接受流。
		ss, err := newSess.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}

		const size = int(2 * initialStreamWindow)
		done := make(chan error, 1)
		go func() {
			_, e := old.write(line, pattern(42, size))
			if e == nil {
				old.closeWrite(line)
			}
			done <- e
		}()

		got := make([]byte, size)
		if _, err := io.ReadFull(ss, got); err != nil {
			t.Fatal(err)
		}
		checkPattern(t, got, 42)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("old sender stalled")
		}
		if _, err := ss.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("expect EOF, got %v", err)
		}
		if old.zero {
			t.Fatal("announcement frame must not create a stream 0 on old peers")
		}
	})

	t.Run("new_sends_old_receives", func(t *testing.T) {
		conf := testWindowConf(initialStreamWindow)
		_ = captureLogs(conf)
		c1, c2 := testConnPipe(t)
		newSess, err := Server(c1, conf)
		if err != nil {
			t.Fatal(err)
		}
		old := newOldPeer(t, c2, true)
		defer old.close()
		defer newSess.Close()

		cs, err := newSess.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		_, line := old.accept()

		const size = int(2 * initialStreamWindow)
		done := asyncWrite(cs, pattern(77, size))
		got := make([]byte, size)
		off := 0
		chunk := make([]byte, 5000)
		for off < size {
			n, rerr := old.oldRead(line, chunk)
			if rerr != nil {
				t.Fatal(rerr)
			}
			copy(got[off:], chunk[:n])
			off += n
		}
		checkPattern(t, got, 77)
		assertDone(t, done, 5*time.Second, "new sender transfer")
		_ = cs.Close()
		// FIN 后旧端读到 EOF。
		if _, rerr := old.oldRead(line, chunk); rerr != io.EOF {
			t.Fatalf("expect EOF, got %v", rerr)
		}
		if old.zero {
			t.Fatal("no stream 0 SYN may ever reach an old peer")
		}
	})

	t.Run("ping", func(t *testing.T) {
		conf := testWindowConf(initialStreamWindow)
		_ = captureLogs(conf)
		c1, c2 := testConnPipe(t)
		newSess, err := Client(c1, conf)
		if err != nil {
			t.Fatal(err)
		}
		old := newOldPeer(t, c2, false)
		defer old.close()
		defer newSess.Close()
		if _, err := newSess.Ping(); err != nil {
			t.Fatalf("ping against old peer: %v", err)
		}
	})
}
