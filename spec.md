# Specification

We use this document to detail the internal specification of Yamux.
This is used both as a guide for implementing Yamux, but also for
alternative interoperable libraries to be built.

# Framing

Yamux uses a streaming connection underneath, but imposes a message
framing so that it can be shared between many logical streams. Each
frame contains a header like:

* Version (8 bits)
* Type (8 bits)
* Flags (16 bits)
* StreamID (32 bits)
* Length (32 bits)

This means that each header has a 12 byte overhead.
All fields are encoded in network order (big endian).
Each field is described below:

## Version Field

The version field is used for future backward compatibility. At the
current time, the field is always set to 0, to indicate the initial
version.

## Type Field

The type field is used to switch the frame message type. The following
message types are supported:

* 0x0 Data - Used to transmit data. May transmit zero length payloads
  depending on the flags.

* 0x1 Window Update - Used to updated the senders receive window size.
  This is used to implement per-session flow control.

* 0x2 Ping - Used to measure RTT. It can also be used to heart-beat
  and do keep-alives over TCP.

* 0x3 Go Away - Used to close a session.

## Flag Field

The flags field is used to provide additional information related
to the message type. The following flags are supported:

* 0x1 SYN - Signals the start of a new stream. May be sent with a data or
  window update message. Also sent with a ping to indicate outbound.

* 0x2 ACK - Acknowledges the start of a new stream. May be sent with a data
  or window update message. Also sent with a ping to indicate response.

* 0x4 FIN - Performs a half-close of a stream. May be sent with a data
  message or window update.

* 0x8 RST - Reset a stream immediately. May be sent with a data or
  window update message.

## StreamID Field

The StreamID field is used to identify the logical stream the frame
is addressing. The client side should use odd ID's, and the server even.
This prevents any collisions. Additionally, the 0 ID is reserved to represent
the session.

Both Ping and Go Away messages should always use the 0 StreamID.

## Length Field

The meaning of the length field depends on the message type:

* Data - provides the length of bytes following the header
* Window update - provides a delta update to the window size
* Ping - Contains an opaque value, echoed back
* Go Away - Contains an error code

# Message Flow

There is no explicit connection setup, as Yamux relies on an underlying
transport to be provided. However, there is a distinction between client
and server side of the connection.

## Opening a stream

To open a stream, an initial data or window update frame is sent
with a new StreamID. The SYN flag should be set to signal a new stream.

The receiver must then reply with either a data or window update frame
with the StreamID along with the ACK flag to accept the stream or with
the RST flag to reject the stream.

Because we are relying on the reliable stream underneath, a connection
can begin sending data once the SYN flag is sent. The corresponding
ACK does not need to be received. This is particularly well suited
for an RPC system where a client wants to open a stream and immediately
fire a request without waiting for the RTT of the ACK.

This does introduce the possibility of a connection being rejected
after data has been sent already. This is a slight semantic difference
from TCP, where the connection cannot be refused after it is opened.
Clients should be prepared to handle this by checking for an error
that indicates a RST was received.

## Closing a stream

To close a stream, either side sends a data or window update frame
along with the FIN flag. This does a half-close indicating the sender
will send no further data.

Once both sides have closed the connection, the stream is closed.

Alternatively, if an error occurs, the RST flag can be used to
hard close a stream immediately.

## Flow Control

When Yamux is initially starts each stream with a 256KB window size.
There is no window size for the session.

To prevent the streams from stalling, window update frames should be
sent regularly. Yamux can be configured to provide a larger limit for
windows sizes. Both sides assume the initial 256KB window, but can
immediately send a window update as part of the SYN/ACK indicating a
larger window.

Both sides should track the number of bytes sent in Data frames
only, as only they are tracked as part of the window size.

## 可选的会话级接收窗口（Session Receive Window，可选扩展）

单流窗口只能约束单条流。当一条会话上复用大量流且接收端读取缓慢时，
每条流的未读数据仍可各自累积到单流窗口上限。为把“整条会话的未读数据
总量”约束在接收端额度内，实现提供了可选的会话级接收窗口扩展
（对应配置 `Config.MaxSessionReceiveWindowSize`，取 0 表示不启用，
保持本文件前述的原有行为）。

该扩展完全复用既有帧格式，不引入新的消息类型：

* 通告帧（能力协商 + 初始窗口）：接收端启用后，会把该帧作为会话发出的
  第一帧。帧内容为 Type=`Window Update`、Flags=`ACK`、StreamID=`0`、
  Length=`MaxSessionReceiveWindowSize`。发送端收到该帧后激活会话级
  发送额度，初始额度为通告窗口减去激活前已经发出的数据字节数（这部分
  在途字节以“额度债务”形式记账，会随接收端返还逐步弥补，不会永久丢失）。
* 增量返还帧：Type=`Window Update`、Flags=`0`、StreamID=`0`、
  Length=本次返还的字节数 delta。接收端在任意流的数据被应用读走，或
  含未读数据的流被完全关闭（FIN 双向完成或 RST）时返还；额度曾经耗尽
  时任意数量都立即返还，否则按半窗口批量返还。

语义与约束：

* 仅当接收端启用（发出通告）时，对应方向才存在会话级约束；未启用或
  旧版本对端不发送通告，发送端始终按单流窗口工作。
* 旧版本对端收到 StreamID=`0` 的 Window Update 时，会把它作为
  “未知流的窗口更新”安全忽略，不会创建流、不会断连，因此支持灰度
  开启与新旧端互通。
* 会话额度耗尽时数据帧写入在发送端排队等待，但 Ping、Window Update、
  GoAway 等控制帧不经过额度预留，不会被等待中的数据阻塞；已建立的流
  不会仅因额度耗尽而被重置。
* 大写入随额度返还分块完成，字节顺序与完整性不变；写入超时、流关闭或
  会话关闭后不会产生迟到数据、永久占用的额度或永久卡住的写入。

## Session termination

When a session is being terminated, the Go Away message should
be sent. The Length should be set to one of the following to
provide an error code:

* 0x0 Normal termination
* 0x1 Protocol error
* 0x2 Internal error
