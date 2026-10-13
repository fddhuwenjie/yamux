## Unreleased

### Improvements

- 新增可选配置 `Config.MaxSessionReceiveWindowSize`：在单流窗口之上再
  施加一层会话级接收窗口，把整条会话中“已接收但尚未被任何流读取”的
  数据总量约束在接收端额度内。零值保持原有行为；仅一端启用时与旧版本
  对端完全兼容（复用 StreamID=0 的 Window Update 帧，旧端会安全忽略）。
  额度耗尽时数据写入等待，读取任意流或完全关闭含未读数据的流后继续；
  Ping 等控制帧不受阻塞，写入超时/流关闭/会话关闭后不残留迟到数据、
  占用额度或卡住的写入。

### Changes

### Fixed

### Security
