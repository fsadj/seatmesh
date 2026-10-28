#pragma once
// ============================================================
// SeatMesh 引擎全局契约（依赖反转核心，本文件即"宪法"）
//
// 规则：
//  1. engine 核心只允许 include 本头文件与 C++ 标准库；
//     禁止 include zmq / sqlite / protobuf —— 具体技术只出现在
//     src/storage 与 src/adapters，由 src/main.cpp（组合根）装配。
//  2. 修改本文件 = 修改契约：M9 汇总、M1+M4 签字、契约测试回归、epoch+1。
//  3. 时间一律 UNIX 毫秒；位运算语义遵循 docs/00 时间轴（06:00-22:00，
//     15min/片，每天 64 片，一个座位一天 = 一个 uint64）。
//  4. append 在事件持久后通过 set_apply 回调投递：单机实现同步调用，
//     共识实现（RaftLog）在多数派 commit 后调用——apply 是唯一线性化点。
// ============================================================
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace seatmesh {

// ---------- 常量（与 protos/scheduler.proto 绑定，代码不得另立） ----------
// 语义对齐真实校园图书馆选座系统（平行实现，不连接任何真实系统）。
constexpr int    kSlotsPerDay    = 64;
constexpr int    kSlotMinutes    = 15;
constexpr int    kDayStartMin    = 6 * 60;            // 06:00 营业开始
constexpr int    kDayEndMin      = 22 * 60;           // 22:00（16h = 64 片）
constexpr int    kRollingDays    = 14;                // day_off ∈ [0,13]
constexpr int    kMaxSlotsPerReq = 16;                // 单次最多 4 小时；0..960 = 全天资格
// 入馆资格：申请成功后 30min 内入馆刷卡；当日 08:00 前的申请保留至当日 08:30
constexpr int64_t kEntryGraceMs         = 30 * 60 * 1000;
constexpr int64_t kMorningHoldUntilMs   = (8 * 60 + 30) * 60 * 1000;  // 当日 08:30（绝对时刻由引擎按日锚点换算）
constexpr int64_t kMorningApplyBeforeMs = 8 * 60 * 60 * 1000;         // 当日 08:00
// 中途离开保留期：11:00–13:00 / 17:00–19:00 为 60min，其余 30min
constexpr int64_t kBreakawayKeepMs     = 30 * 60 * 1000;
constexpr int64_t kBreakawayKeepPeakMs = 60 * 60 * 1000;
constexpr int    kBreakawayPeakOpenMin  = 11 * 60;
constexpr int    kBreakawayPeakCloseMin = 13 * 60;    // 第二段 17:00–19:00 由引擎按同规则判断
// 取消/爽约后 30min 冷却；爽约（NO_SHOW）当日不可再线上申请（网关硬规则）
constexpr int64_t kCancelCooldownMs = 30 * 60 * 1000;
constexpr int    kApplyOpenMin  = 6 * 60;             // 06:00 开放申请
constexpr int    kApplyCloseMin = 21 * 60 + 45;       // 21:45 截止
// 扩展（本系统特有，馆方无对应机制）
constexpr int64_t kGrantTtlMs    = 60 * 1000;         // 候补授权期
constexpr int64_t kSagaTimeoutMs = 30 * 1000;         // 跨区配额确认

// ---------- 事件（日志与 apply 的唯一记录类型） ----------
enum class EventType : uint8_t {
  RESERVE = 1, CANCEL, CHECKIN, CHECKOUT, NO_SHOW,
  GRANT_OFFER, GRANT_CLAIM, GRANT_EXPIRE,
  FREEZE, SAGA, SYSTEM, BREAKAWAY = 12, RETURN = 13
};

struct Event {
  EventType  type = EventType::SYSTEM;
  uint64_t   seq = 0;              // apply 时分配，单调递增（跨副本确定性）
  int64_t    ts_ms = 0;
  int64_t    user_id = 0;
  int32_t    seat_id = -1;
  int32_t    day_off = -1;
  int32_t    start_min = -1;       // 距 06:00 的分钟，15 的倍数
  int32_t    end_min = -1;         // 不含端点
  uint64_t   mask = 0;             // 涉及的时间片
  uint64_t   released_mask = 0;    // CHECKOUT：释放的剩余时段
  std::string request_id;          // 幂等键（UUIDv4）
  std::string booking_id;
  std::string queue_ticket;
  std::string trace_id;            // 16B hex，贯穿网关→引擎→事件流
  std::string detail;              // 拒绝原因 / 人类可读补充
};

// ---------- 入站请求（由 IRequestSource 投喂，未成事件） ----------
struct IncomingRequest {
  std::string kind;   // "reserve" | "cancel" | "checkin" | "checkout" | "quit"
  Event ev;           // 复用字段载体，kind 决定哪些字段有效
};

// ---------- IClock：可注入时钟（确定性仿真测试 DST 的前提） ----------
class IClock {
 public:
  virtual ~IClock() = default;
  virtual int64_t now_ms() const = 0;
};
class SystemClock final : public IClock {
 public:
  int64_t now_ms() const override;
};
class FakeClock final : public IClock {
 public:
  explicit FakeClock(int64_t start_ms) : now_(start_ms) {}
  int64_t now_ms() const override { return now_; }
  void advance(int64_t ms) { now_ += ms; }
 private:
  int64_t now_;
};

// ---------- IEventLog：追加日志（文件 / 数据库 / 内存三实现，可切换） ----------
using ApplyFn = std::function<void(const Event&, uint64_t index)>;
using SnapshotOut  = std::function<std::string()>;             // 引擎导出状态
using SnapshotIn   = std::function<void(const std::string&)>;  // 引擎恢复状态

class IEventLog {
 public:
  virtual ~IEventLog() = default;
  // 追加事件；持久后（本契约下同步）回调 apply 并返回 log index。
  // 事件内容不允许修改——日志是不可变事实。
  virtual uint64_t append(const Event& e) = 0;
  // 刷盘屏障：此后该 log 的所有 append 均已 durable。批量优化时的统一提交点。
  virtual void barrier() = 0;
  virtual void set_apply(ApplyFn fn) = 0;
  // 快照提供者（不支持快照的实现可空操作）
  virtual void set_snapshot(SnapshotOut out, SnapshotIn in) { (void)out; (void)in; }
  virtual bool write_snapshot() { return false; }
  // 启动重放：先恢复快照，再逐条回放（经 set_apply 回调）；尾部损坏自动截断。
  // 返回最后 log index。
  virtual uint64_t replay() = 0;
  virtual uint64_t applied_index() const = 0;
};

// ---------- IProjectionSink：apply 之后的只读投影（查询/对账镜像） ----------
// 投影失败不得阻断主流程（日志才是事实，投影只是视图）。
class IProjectionSink {
 public:
  virtual ~IProjectionSink() = default;
  virtual void on_apply(const Event& e, uint64_t index) = 0;
  virtual const char* name() const = 0;
};

// ---------- IEventPublisher：事件流 PUB（网关/大屏/AI/审计器订阅） ----------
class IEventPublisher {
 public:
  virtual ~IEventPublisher() = default;
  virtual void publish(const Event& e) = 0;
};

// ---------- ITimerWheel：定时器（签到超时/授权期/Saga 超时共用） ----------
struct Timer {
  uint64_t id = 0;
  int64_t  deadline_ms = 0;
  std::string tag;   // 约定 "expire:<booking_id>" 等
};
class ITimerWheel {
 public:
  virtual ~ITimerWheel() = default;
  virtual uint64_t schedule(int64_t deadline_ms, std::string tag) = 0;
  virtual void cancel(uint64_t id) = 0;
  // 返回 now_ms 前到期且未取消的定时器，按 deadline 升序（确定性）
  virtual std::vector<Timer> expire(int64_t now_ms) = 0;
};

// ---------- IRequestSource：入站请求源（stdin / 回放文件 / ZMQ） ----------
class IRequestSource {
 public:
  virtual ~IRequestSource() = default;
  // 非阻塞取一条请求；无请求返回 nullopt
  virtual std::optional<IncomingRequest> poll() = 0;
  // 回放源在耗尽后返回 true（stdin 永远 false）
  virtual bool exhausted() const { return false; }
};

// ---------- 处理结果（引擎同步应答；共识版为 propose 后的最终结果） ----------
struct Outcome {
  bool        ok = false;
  std::string booking_id;
  uint64_t    conflict_mask = 0;   // 冲突时：与请求重叠的已占片
  std::string detail;
};

}  // namespace seatmesh
