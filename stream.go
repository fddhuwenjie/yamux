// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type streamState int

const (
	streamInit streamState = iota
	streamSYNSent
	streamSYNReceived
	streamEstablished
	streamLocalClose
	streamRemoteClose
	streamClosed
	streamReset
)

// Stream is used to represent a logical stream within a session. Methods on
// Stream are safe to call concurrently with one another, but all Read calls
// must be on the same goroutine and all Write calls must be on the same
// goroutine.
type Stream struct {
	recvWindow uint32
	sendWindow uint32

	id      uint32
	session *Session

	state     streamState
	stateLock sync.Mutex

	recvBuf  *bytes.Buffer
	recvLock sync.Mutex

	// sessionCreditsReleased is set while recvLock is held when a stream is
	// fully closed: the unread bytes still in recvBuf were returned to the
	// session receive window wholesale, so a later Read that drains them must
	// not return the same session credits a second time.
	sessionCreditsReleased bool

	controlHdr     header
	controlErr     chan error
	controlHdrLock sync.Mutex

	sendHdr  header
	sendErr  chan error
	sendLock sync.Mutex

	recvNotifyCh chan struct{}
	sendNotifyCh chan struct{}

	readDeadline  atomic.Value // time.Time
	writeDeadline atomic.Value // time.Time

	// establishCh is notified if the stream is established or being closed.
	establishCh chan struct{}

	// closeTimer is set with stateLock held to honor the StreamCloseTimeout
	// setting on Session.
	closeTimer *time.Timer
}

// newStream is used to construct a new stream within
// a given session for an ID
func newStream(session *Session, id uint32, state streamState) *Stream {
	s := &Stream{
		id:           id,
		session:      session,
		state:        state,
		controlHdr:   header(make([]byte, headerSize)),
		controlErr:   make(chan error, 1),
		sendHdr:      header(make([]byte, headerSize)),
		sendErr:      make(chan error, 1),
		recvWindow:   initialStreamWindow,
		sendWindow:   initialStreamWindow,
		recvNotifyCh: make(chan struct{}, 1),
		sendNotifyCh: make(chan struct{}, 1),
		establishCh:  make(chan struct{}, 1),
	}
	s.readDeadline.Store(time.Time{})
	s.writeDeadline.Store(time.Time{})
	return s
}

// Session returns the associated stream session
func (s *Stream) Session() *Session {
	return s.session
}

// StreamID returns the ID of this stream
func (s *Stream) StreamID() uint32 {
	return s.id
}

// Read is used to read from the stream. It is safe to call Write, Read, and/or
// Close concurrently with each other, but calls to Read are not reentrant and
// should not be called from multiple goroutines. Multiple Read goroutines would
// receive different chunks of data from the Stream and be unable to reassemble
// them in order or along message boundaries, and may encounter deadlocks.
func (s *Stream) Read(b []byte) (n int, err error) {
	defer asyncNotify(s.recvNotifyCh)
	// creditsReleased records whether the bytes read had their session
	// credits returned already when the stream was fully closed; declared
	// before the START label because goto must not jump over it.
	var creditsReleased bool
START:

	// If the stream is closed and there's no data buffered, return EOF
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose:
		// LocalClose only prohibits further local writes. Handle reads normally.
	case streamRemoteClose:
		fallthrough
	case streamClosed:
		s.recvLock.Lock()
		if s.recvBuf == nil || s.recvBuf.Len() == 0 {
			s.recvLock.Unlock()
			s.stateLock.Unlock()
			return 0, io.EOF
		}
		s.recvLock.Unlock()
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	s.recvLock.Lock()
	if s.recvBuf == nil || s.recvBuf.Len() == 0 {
		s.recvLock.Unlock()
		goto WAIT
	}

	// Read any bytes
	n, _ = s.recvBuf.Read(b)
	creditsReleased = s.sessionCreditsReleased
	s.recvLock.Unlock()

	// Bytes consumed by the application can be returned as session wide
	// receive window credits to peers blocked on the shared quota. If the
	// stream was fully closed first, these bytes were already returned
	// wholesale then and must not be credited twice.
	if !creditsReleased {
		s.session.noteSessionDataConsumed(uint32(n))
	}

	// Send a window update potentially
	err = s.sendWindowUpdate()
	if err == ErrSessionShutdown {
		err = nil
	}
	return n, err

WAIT:
	var timeout <-chan time.Time
	var timer *time.Timer
	readDeadline := s.readDeadline.Load().(time.Time)
	if !readDeadline.IsZero() {
		delay := time.Until(readDeadline)
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	select {
	case <-s.session.shutdownCh:
	case <-s.recvNotifyCh:
	case <-timeout:
		return 0, ErrTimeout
	}
	if timer != nil {
		if !timer.Stop() {
			<-timeout
		}
	}
	goto START
}

// Write is used to write to the stream. It is safe to call Write, Read, and/or
// Close concurrently with each other, but calls to Write are not reentrant and
// should not be called from multiple goroutines.
func (s *Stream) Write(b []byte) (n int, err error) {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	total := 0
	for total < len(b) {
		n, err := s.write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// write is used to write to the stream, may return on
// a short write.
func (s *Stream) write(b []byte) (n int, err error) {
	var flags uint16
	var max uint32
	var body []byte
	// sessionReserved records whether the current chunk had session level
	// receive window credits reserved, so accounting can be finalized exactly
	// once after the send completes or fails.
	var sessionReserved bool
	// peerSessionNotify holds the notification channel captured atomically
	// together with an "exhausted" credit decision; waiting must use this
	// exact channel to avoid missing a credit update between decision and
	// wait. It is nil when waiting for the per-stream window only.
	var peerSessionNotify <-chan struct{}
START:
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose:
		fallthrough
	case streamClosed:
		s.stateLock.Unlock()
		return 0, ErrStreamClosed
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	window := atomic.LoadUint32(&s.sendWindow)
	if window == 0 {
		peerSessionNotify = nil
		goto WAIT
	}

	// Determine the flags if any
	flags = s.sendFlags()

	// Send up to our send window
	max = min(window, uint32(len(b)))

	// Additionally constrain the chunk by the peer's session wide receive
	// window when the peer enabled session level flow control. A zero grant
	// means the session quota is exhausted (other streams may hold it); wait
	// for credits without queuing any data or resetting this stream.
	max, sessionReserved, peerSessionNotify = s.session.reservePeerSessionOrWait(max)
	if max == 0 {
		goto WAIT
	}
	body = b[:max]

	// Send the header
	s.sendHdr.encode(typeData, flags, s.id, max)
	if err = s.session.waitForSendErr(s.sendHdr, body, s.sendErr); err != nil {
		// Give reserved session credits back so no quota is leaked and other
		// streams can proceed; no data of this chunk is accounted as sent.
		s.session.refundPeerSession(max, sessionReserved)
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.sendHdr = header(make([]byte, headerSize))
		}
		return 0, err
	}

	// Reduce our send window
	atomic.AddUint32(&s.sendWindow, ^uint32(max-1))

	// Finalize session window accounting (pre-activation bytes are tracked
	// separately so activation can compute the exact initial quota).
	s.session.commitPeerSessionSent(max, sessionReserved)

	// Unlock
	return int(max), err

WAIT:
	var timeout <-chan time.Time
	var timer *time.Timer
	writeDeadline := s.writeDeadline.Load().(time.Time)
	if !writeDeadline.IsZero() {
		delay := time.Until(writeDeadline)
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	// When waiting on the per-stream window only, snapshot the session
	// notification channel here; when session credits were the blocker,
	// peerSessionNotify was captured atomically with the exhaustion decision
	// so no in-between credit update can be missed.
	if peerSessionNotify == nil {
		_, _, peerSessionNotify = s.session.peerSessionSnapshot()
	}
	select {
	case <-s.session.shutdownCh:
	case <-s.sendNotifyCh:
	case <-peerSessionNotify:
	case <-timeout:
		return 0, ErrTimeout
	}
	if timer != nil {
		if !timer.Stop() {
			<-timeout
		}
	}
	sessionReserved = false
	peerSessionNotify = nil
	goto START
}

// sendFlags determines any flags that are appropriate
// based on the current stream state
func (s *Stream) sendFlags() uint16 {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	var flags uint16
	switch s.state {
	case streamInit:
		flags |= flagSYN
		s.state = streamSYNSent
	case streamSYNReceived:
		flags |= flagACK
		s.state = streamEstablished
	}
	return flags
}

// sendWindowUpdate potentially sends a window update enabling
// further writes to take place. Must be invoked with the lock.
func (s *Stream) sendWindowUpdate() error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	// Determine the delta update
	max := s.session.config.MaxStreamWindowSize
	var bufLen uint32
	s.recvLock.Lock()
	if s.recvBuf != nil {
		bufLen = uint32(s.recvBuf.Len())
	}
	delta := (max - bufLen) - s.recvWindow

	// Determine the flags if any
	flags := s.sendFlags()

	// Check if we can omit the update
	if delta < (max/2) && flags == 0 {
		s.recvLock.Unlock()
		return nil
	}

	// Update our window
	s.recvWindow += delta
	s.recvLock.Unlock()

	// Send the header
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, delta)
	if err := s.session.waitForSendErr(s.controlHdr, nil, s.controlErr); err != nil {
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.controlHdr = header(make([]byte, headerSize))
		}
		return err
	}
	return nil
}

// sendClose is used to send a FIN
func (s *Stream) sendClose() error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	flags := s.sendFlags()
	flags |= flagFIN
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, 0)
	if err := s.session.waitForSendErr(s.controlHdr, nil, s.controlErr); err != nil {
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.controlHdr = header(make([]byte, headerSize))
		}
		return err
	}
	return nil
}

// Close is used to close the stream. It is safe to call Close concurrently.
func (s *Stream) Close() error {
	closeStream := false
	s.stateLock.Lock()
	switch s.state {
	// Opened means we need to signal a close
	case streamSYNSent:
		fallthrough
	case streamSYNReceived:
		fallthrough
	case streamEstablished:
		s.state = streamLocalClose
		goto SEND_CLOSE

	case streamLocalClose:
	case streamRemoteClose:
		s.state = streamClosed
		closeStream = true
		goto SEND_CLOSE

	case streamClosed:
	case streamReset:
	default:
		panic("unhandled state")
	}
	s.stateLock.Unlock()
	return nil
SEND_CLOSE:
	// This shouldn't happen (the more realistic scenario to cancel the
	// timer is via processFlags) but just in case this ever happens, we
	// cancel the timer to prevent dangling timers.
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}

	// If we have a StreamCloseTimeout set we start the timeout timer.
	// We do this only if we're not already closing the stream since that
	// means this was a graceful close.
	//
	// This prevents memory leaks if one side (this side) closes and the
	// remote side poorly behaves and never responds with a FIN to complete
	// the close. After the specified timeout, we clean our resources up no
	// matter what.
	if !closeStream && s.session.config.StreamCloseTimeout > 0 {
		s.closeTimer = time.AfterFunc(
			s.session.config.StreamCloseTimeout, s.closeTimeout)
	}

	s.stateLock.Unlock()
	_ = s.sendClose()
	s.notifyWaiting()
	if closeStream {
		s.session.closeStream(s.id)
	}
	return nil
}

// closeTimeout is called after StreamCloseTimeout during a close to
// close this stream.
func (s *Stream) closeTimeout() {
	// Close our side forcibly
	s.forceClose()

	// Free the stream from the session map
	s.session.closeStream(s.id)

	// Send a RST so the remote side closes too.
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeWindowUpdate, flagRST, s.id, 0)
	_ = s.session.sendNoWait(hdr)
}

// forceClose is used for when the session is exiting
func (s *Stream) forceClose() {
	s.stateLock.Lock()
	s.state = streamClosed
	s.stateLock.Unlock()
	s.notifyWaiting()
}

// processFlags is used to update the state of the stream
// based on set flags, if any.
func (s *Stream) processFlags(flags uint16) error {
	s.stateLock.Lock()

	// closeStream is set when the stream must be removed from the session
	// after the state transition. The actual close (which may take the
	// receive lock to release session credits) must run only AFTER
	// stateLock is released: Read takes recvLock before stateLock in
	// sendWindowUpdate, so nesting stateLock inside recvLock the other way
	// around would deadlock a reader against a FIN/RST being processed.
	closeStream := false

	if flags&flagACK == flagACK {
		if s.state == streamSYNSent {
			s.state = streamEstablished
		}
		asyncNotify(s.establishCh)
		s.session.establishStream(s.id)
	}
	if flags&flagFIN == flagFIN {
		switch s.state {
		case streamSYNSent:
			fallthrough
		case streamSYNReceived:
			fallthrough
		case streamEstablished:
			s.state = streamRemoteClose
			s.notifyWaiting()
		case streamLocalClose:
			s.state = streamClosed
			closeStream = true
			s.notifyWaiting()
		default:
			s.session.logger.Printf("[ERR] yamux: unexpected FIN flag in state %d", s.state)
			s.stateLock.Unlock()
			return ErrUnexpectedFlag
		}
	}
	if flags&flagRST == flagRST {
		s.state = streamReset
		closeStream = true
		s.notifyWaiting()
	}
	s.stateLock.Unlock()

	if closeStream {
		if s.closeTimer != nil {
			// Stop our close timeout timer since we gracefully closed
			s.closeTimer.Stop()
		}

		s.session.closeStream(s.id)
	}
	return nil
}

// notifyWaiting notifies all the waiting channels
func (s *Stream) notifyWaiting() {
	asyncNotify(s.recvNotifyCh)
	asyncNotify(s.sendNotifyCh)
	asyncNotify(s.establishCh)
}

// incrSendWindow updates the size of our send window
func (s *Stream) incrSendWindow(hdr header, flags uint16) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Increase window, unblock a sender
	atomic.AddUint32(&s.sendWindow, hdr.Length())
	asyncNotify(s.sendNotifyCh)
	return nil
}

// readData is used to handle a data frame
func (s *Stream) readData(hdr header, flags uint16, conn io.Reader) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Check that our recv window is not exceeded
	length := hdr.Length()
	if length == 0 {
		return nil
	}

	// Wrap in a limited reader
	conn = &io.LimitedReader{R: conn, N: int64(length)}

	// Copy into buffer
	s.recvLock.Lock()

	// A fully closed stream may already have had its buffered bytes returned
	// wholesale to the session receive window (e.g. closeTimeout racing with
	// a data frame already dispatched to this stream). Such late data must
	// not re-enter the buffer: discard it from the wire and give its session
	// credits back, so they are neither leaked nor credited twice. The bytes
	// never entered a buffer, so local receive accounting is untouched.
	if s.sessionCreditsReleased {
		s.recvLock.Unlock()
		if _, err := io.Copy(io.Discard, conn); err != nil {
			s.session.logger.Printf("[ERR] yamux: Failed to discard stream data: %v", err)
			return err
		}
		s.session.returnUnchargedSessionRecv(length)
		asyncNotify(s.recvNotifyCh)
		return nil
	}

	if length > s.recvWindow {
		s.session.logger.Printf("[ERR] yamux: receive window exceeded (stream: %d, remain: %d, recv: %d)", s.id, s.recvWindow, length)
		s.recvLock.Unlock()
		return ErrRecvWindowExceeded
	}

	if s.recvBuf == nil {
		// Allocate the receive buffer just-in-time to fit the full data frame.
		// This way we can read in the whole packet without further allocations.
		s.recvBuf = bytes.NewBuffer(make([]byte, 0, length))
	}
	copiedLength, err := io.Copy(s.recvBuf, conn)
	if err != nil {
		s.session.logger.Printf("[ERR] yamux: Failed to read stream data: %v", err)
		s.recvLock.Unlock()
		return err
	}

	// Decrement the receive window
	s.recvWindow -= uint32(copiedLength)
	s.recvLock.Unlock()

	// Charge the buffered bytes against the session wide receive window so
	// unread data across all streams shares the one receiver quota.
	s.session.noteSessionDataBuffered(uint32(copiedLength))

	// Unblock any readers
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetDeadline sets the read and write deadlines
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// SetReadDeadline sets the deadline for blocked and future Read calls.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetWriteDeadline sets the deadline for blocked and future Write calls
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	asyncNotify(s.sendNotifyCh)
	return nil
}

// Shrink is used to compact the amount of buffers utilized
// This is useful when using Yamux in a connection pool to reduce
// the idle memory utilization.
func (s *Stream) Shrink() {
	s.recvLock.Lock()
	if s.recvBuf != nil && s.recvBuf.Len() == 0 {
		s.recvBuf = nil
	}
	s.recvLock.Unlock()
}
