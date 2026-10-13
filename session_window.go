// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

// 本文件实现可选的会话级接收窗口流控（见 Config.MaxSessionReceiveWindowSize）。
//
// 单流接收窗口只能约束单个流：当一条 yamux 会话上复用大量流且接收端读取缓慢
// 时，每个流的未读数据都可以各自累积到单流窗口上限，内存压力与队头阻塞会随
// 流数线性放大。会话级窗口在整条会话维度上再施加一层“接收端额度”：
//
//   - 接收端启用（本地配置非零）后，会在会话的第一帧发出一次性通告
//     （typeWindowUpdate / flagACK / StreamID=0 / Length=窗口大小）；
//   - 发送端收到通告后激活会话额度记账，所有流的数据帧共享该额度；
//   - 额度耗尽时 Stream.Write 阻塞等待，但不影响 Ping、窗口更新等控制帧；
//   - 接收端读走任意流的数据、或完全关闭仍带未读数据的流时，通过增量窗口
//     更新（typeWindowUpdate / flags=0 / StreamID=0 / Length=delta）返还额度；
//   - 零值配置不启用、不通告；旧版本对端无法识别 StreamID=0 的 WindowUpdate，
//     会按“缺失流的帧”忽略，因此双向兼容、可灰度开启。
//
// 所有会话级窗口状态均为 Session 私有，不同会话之间天然互不影响。

// initSessionReceiveWindow 在会话后台 goroutine 启动前初始化接收端会话窗口，
// 并把通告帧排入发送队列。此时发送队列为空且具备缓冲，通告必然成为整条会话
// 发出的第一帧，保证对端在收到任何数据帧之前先收到通告。
func (s *Session) initSessionReceiveWindow() {
	size := s.config.MaxSessionReceiveWindowSize
	if size == 0 {
		return
	}
	s.sessionRecvOn = true
	s.sessionRecvCredit = int64(size)

	hdr := header(make([]byte, headerSize))
	hdr.encode(typeWindowUpdate, flagACK, sessionControlStreamID, size)
	s.sendCh <- &sendReady{Hdr: hdr}
}

// noteSessionDataBuffered 在一帧数据进入某流的接收缓冲后扣减全会话剩余额度。
// 合作的发送端不会超额发送；但发送端可能在处理本端通告前就已发出首批数据
// （通告与首批数据在两个方向上没有 happens-before 关系），因此剩余额度允许
// 短暂为负，接收端不据此重置流或断开会话，待读取返还后自然回补。
func (s *Session) noteSessionDataBuffered(n uint32) {
	if n == 0 || !s.sessionRecvOn {
		return
	}
	s.sessionRecvMu.Lock()
	s.sessionRecvCredit -= int64(n)
	s.sessionRecvMu.Unlock()
}

// noteSessionDataConsumed 在应用层从任意流读走数据后累计待返还额度，并按
// 阈值决定是否立即发送会话窗口更新：额度仍充裕时按半窗口批量返还以减少帧
// 数量；一旦额度曾经耗尽（剩余 <= 0），任意数量的返还都立即发出，以便及时
// 唤醒因额度耗尽而阻塞的写入者。
func (s *Session) noteSessionDataConsumed(n uint32) {
	if n == 0 || !s.sessionRecvOn {
		return
	}
	s.sessionRecvMu.Lock()
	s.sessionRecvPending += n
	delta := s.prepareSessionRecvFlushLocked(false)
	s.sessionRecvMu.Unlock()

	s.completeSessionRecvFlush(delta)
}

// releaseSessionRecv 用于“数据未被读取但已被丢弃”的场景（流被完全关闭或
// 到达已删除/已重置流的数据被丢弃）。这类字节同样占用过通告额度，必须立即
// （force=true，不受批量阈值限制）返还，避免对端额度被永久占用、其他流的
// 写入永久卡死。
func (s *Session) releaseSessionRecv(n uint32) {
	if n == 0 || !s.sessionRecvOn {
		return
	}
	s.sessionRecvMu.Lock()
	s.sessionRecvPending += n
	delta := s.prepareSessionRecvFlushLocked(true)
	s.sessionRecvMu.Unlock()

	s.completeSessionRecvFlush(delta)
}

// prepareSessionRecvFlushLocked 在持锁状态下判断是否需要发送窗口更新；若需要
// 则把待返还额度清零并返回 delta，未发出期间新产生的待返还额度不受影响。
func (s *Session) prepareSessionRecvFlushLocked(force bool) uint32 {
	delta := s.sessionRecvPending
	if delta == 0 {
		return 0
	}
	if !force {
		half := int64(s.config.MaxSessionReceiveWindowSize / 2)
		if int64(delta) < half && s.sessionRecvCredit > 0 {
			return 0
		}
	}
	s.sessionRecvPending = 0
	return delta
}

// completeSessionRecvFlush 在锁外发送窗口更新帧（发送排队可能触发写超时，
// 不能持锁），并根据结果提交或回滚本地记账。
func (s *Session) completeSessionRecvFlush(delta uint32) {
	if delta == 0 {
		return
	}
	if err := s.sendSessionRecvUpdate(delta); err != nil {
		// 会话关闭或发送队列长时间不可用：回滚待返还额度，等待后续读取时
		// 重试；若会话已经关闭，也不会再有写入者等待，挂账没有副作用。
		s.sessionRecvMu.Lock()
		s.sessionRecvPending += delta
		s.sessionRecvMu.Unlock()
		return
	}
	s.sessionRecvMu.Lock()
	s.sessionRecvCredit += int64(delta)
	s.sessionRecvMu.Unlock()
}

// sendSessionRecvUpdate queues a single session receive window update frame
// carrying delta credits.
func (s *Session) sendSessionRecvUpdate(delta uint32) error {
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeWindowUpdate, 0, sessionControlStreamID, delta)
	return s.sendNoWait(hdr)
}

// returnUnchargedSessionRecv returns credits for data that was charged by the
// sender but never entered any receive buffer on this endpoint: late data for
// a removed/reset stream is drained straight off the wire. Local receive
// accounting must stay untouched for such bytes, otherwise the remaining
// credit figure (used for flush batching) would drift above the configured
// window and defeat eager flushing after a real exhaustion.
func (s *Session) returnUnchargedSessionRecv(n uint32) {
	if n == 0 || !s.sessionRecvOn {
		return
	}
	// Best effort: if the session is shutting down nobody waits for credits.
	_ = s.sendSessionRecvUpdate(n)
}

// reservePeerSessionOrWait 在发送一帧数据前预留对端会话额度。granted 为
// 本次实际允许发送的字节数（额度不足时为 0，调用方应转入等待）；reserved
// 表示预留时会话流控是否已激活，供发送完成/失败后精确记账；notify 在额度
// 不足时返回，它与“额度判定”在同一把锁内原子抓取，调用方必须直接等待该
// 通道而不能事后再抓取新通道——额度更新可能恰好发生在判定与抓取之间，
// 事后抓取会得到已被替换的新通道，错过本次关闭广播而永久休眠。
//
// 未激活时不做任何约束（旧对端行为），notify 为 nil。
func (s *Session) reservePeerSessionOrWait(want uint32) (granted uint32, reserved bool, notify <-chan struct{}) {
	if want == 0 {
		return 0, false, nil
	}
	s.peerSessionMu.Lock()
	defer s.peerSessionMu.Unlock()
	if !s.peerSessionOn {
		return want, false, nil
	}
	if s.peerSessionCredit <= 0 {
		return 0, true, s.peerSessionNotify
	}
	granted = want
	if int64(granted) > s.peerSessionCredit {
		granted = uint32(s.peerSessionCredit)
	}
	s.peerSessionCredit -= int64(granted)
	return granted, true, nil
}

// commitPeerSessionSent 在数据帧确认发送成功后记账。reserved 为 true 时额度
// 已在预留阶段扣减；否则该帧在预留时尚未激活会话流控，需要根据当前状态补记：
// 仍未激活则计入激活前发送量（供通告到达时计算初始额度），恰好与激活交错则
// 直接补扣，保证任何字节都恰好记账一次。
func (s *Session) commitPeerSessionSent(n uint32, reserved bool) {
	if n == 0 || reserved {
		return
	}
	s.peerSessionMu.Lock()
	defer s.peerSessionMu.Unlock()
	if s.peerSessionOn {
		s.peerSessionCredit -= int64(n)
	} else {
		s.peerSessionPreSent += uint64(n)
	}
}

// refundPeerSession 在数据帧发送失败（写超时、会话关闭等）时归还预留额度。
// 未激活的预留从未扣减额度，且失败意味着字节没有发出、也未计入激活前发送量，
// 因此无需处理。归还后广播所有等待者，其他流的写入可以立即使用该额度。
func (s *Session) refundPeerSession(n uint32, reserved bool) {
	if n == 0 || !reserved {
		return
	}
	s.peerSessionMu.Lock()
	s.peerSessionCredit += int64(n)
	s.broadcastPeerSessionLocked()
	s.peerSessionMu.Unlock()
}

// peerSessionSnapshot 返回当前会话流控状态及等待通知通道。调用方在 select 中
// 接收该通道；额度增加时通道会被关闭以广播唤醒全部等待者，并替换为新通道。
func (s *Session) peerSessionSnapshot() (on bool, credit int64, notify <-chan struct{}) {
	s.peerSessionMu.Lock()
	defer s.peerSessionMu.Unlock()
	return s.peerSessionOn, s.peerSessionCredit, s.peerSessionNotify
}

// activatePeerSessionWindow 处理对端的一次性会话窗口通告。激活时把通告到达前
// 已经发送的数据字节计入已用额度；这可能使剩余额度为负（形成“额度债务”），
// 接收端读回这些在途字节后会通过增量更新逐步弥补，额度不会永久丢失。
// 重复通告一律忽略。
func (s *Session) activatePeerSessionWindow(size uint32) {
	if size < initialStreamWindow {
		s.logger.Printf("[WARN] yamux: ignoring session window announcement of %d bytes (minimum %d)", size, initialStreamWindow)
		return
	}
	s.peerSessionMu.Lock()
	defer s.peerSessionMu.Unlock()
	if s.peerSessionOn {
		return
	}
	s.peerSessionOn = true
	s.peerSessionCredit = int64(size) - int64(s.peerSessionPreSent)
}

// addPeerSessionCredit 处理对端的增量会话窗口更新，增加额度并广播唤醒所有
// 因额度耗尽而等待的写入者。
func (s *Session) addPeerSessionCredit(delta uint32) {
	s.peerSessionMu.Lock()
	defer s.peerSessionMu.Unlock()
	if !s.peerSessionOn {
		// 旧版本对端不会发送该帧；没有先行通告却收到增量更新属于异常情况，
		// 忽略而不是断开会话。
		s.logger.Printf("[WARN] yamux: session window update received without announcement, ignored")
		return
	}
	s.peerSessionCredit += int64(delta)
	s.broadcastPeerSessionLocked()
}

// broadcastPeerSessionLocked 关闭当前通知通道以唤醒全部等待者，并放入一个新的
// 通道供后续等待使用（关闭-替换是常用的广播模式）。调用方必须持有
// peerSessionMu。
func (s *Session) broadcastPeerSessionLocked() {
	close(s.peerSessionNotify)
	s.peerSessionNotify = make(chan struct{}, 1)
}

// handleSessionWindowControl 处理 StreamID=0 的会话级窗口控制帧，对旧版本
// 协议中没有语义的帧做安全忽略。
func (s *Session) handleSessionWindowControl(hdr header) error {
	switch hdr.Flags() {
	case flagACK:
		s.activatePeerSessionWindow(hdr.Length())
	case 0:
		s.addPeerSessionCredit(hdr.Length())
	default:
		s.logger.Printf("[WARN] yamux: ignoring session window control frame with unexpected flags: %d", hdr.Flags())
	}
	return nil
}
