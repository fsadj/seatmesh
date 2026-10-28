#include "storage/projections.hpp"

#include "engine/booking.hpp"
#include "engine/event_codec.hpp"

#include <sqlite3.h>

#include <cstdarg>
#include <stdexcept>

namespace seatmesh {

namespace {
const char* type_name(EventType t) {
  switch (t) {
    case EventType::RESERVE: return "RESERVE";
    case EventType::CANCEL: return "CANCEL";
    case EventType::CHECKIN: return "CHECKIN";
    case EventType::CHECKOUT: return "CHECKOUT";
    case EventType::NO_SHOW: return "NO_SHOW";
    case EventType::GRANT_OFFER: return "GRANT_OFFER";
    case EventType::GRANT_CLAIM: return "GRANT_CLAIM";
    case EventType::GRANT_EXPIRE: return "GRANT_EXPIRE";
    case EventType::FREEZE: return "FREEZE";
    case EventType::SAGA: return "SAGA";
    case EventType::SYSTEM: return "SYSTEM";
  }
  return "?";
}
}  // namespace

// ---------------- CsvProjection ----------------

CsvProjection::CsvProjection(std::string path) {
  bool need_header = false;
  if (std::FILE* probe = std::fopen(path.c_str(), "r")) {
    need_header = std::fgetc(probe) == EOF;
    std::fclose(probe);
  } else {
    need_header = true;
  }
  f_ = std::fopen(path.c_str(), "a");
  if (!f_) throw std::runtime_error("cannot open csv: " + path);
  if (need_header)
    std::fprintf(f_,
                 "index,ts_ms,seq,type,booking_id,user_id,seat_id,day_off,"
                 "mask,released_mask,detail\n");
}

CsvProjection::~CsvProjection() {
  if (f_) std::fclose(f_);
}

void CsvProjection::on_apply(const Event& e, uint64_t index) {
  if (!f_) return;
  std::fprintf(f_, "%llu,%lld,%llu,%s,%s,%lld,%d,%d,%llu,%llu,%s\n",
               static_cast<unsigned long long>(index),
               static_cast<long long>(e.ts_ms),
               static_cast<unsigned long long>(e.seq),
               type_name(e.type),
               e.booking_id.c_str(),
               static_cast<long long>(e.user_id), e.seat_id, e.day_off,
               static_cast<unsigned long long>(e.mask),
               static_cast<unsigned long long>(e.released_mask),
               e.detail.c_str());
  std::fflush(f_);
}

// ---------------- SqliteProjection ----------------
// bookings 为可查询镜像（控制面/前端列表用），credit_events 记信用流水。
// 状态名以 booking.hpp 的 state_name 为准。

namespace {

class SqliteProjection final : public IProjectionSink {
 public:
  explicit SqliteProjection(std::string path) {
    if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK)
      throw std::runtime_error("open projection db failed");
    char* err = nullptr;
    if (sqlite3_exec(db_,
                     "CREATE TABLE IF NOT EXISTS bookings("
                     " booking_id TEXT PRIMARY KEY, user_id INTEGER,"
                     " seat_id INTEGER, day_off INTEGER, mask INTEGER,"
                     " state TEXT, updated_ms INTEGER);"
                     "CREATE TABLE IF NOT EXISTS credit_events("
                     " seq INTEGER PRIMARY KEY, user_id INTEGER,"
                     " delta INTEGER, booking_id TEXT);",
                     nullptr, nullptr, &err) != SQLITE_OK) {
      std::string msg = err ? err : "schema failed";
      sqlite3_free(err);
      throw std::runtime_error("projection schema: " + msg);
    }
  }
  ~SqliteProjection() override {
    if (db_) sqlite3_close(db_);
  }

  void on_apply(const Event& e, uint64_t) override {
    switch (e.type) {
      case EventType::RESERVE:
        if (!e.booking_id.empty())
          exec("INSERT OR REPLACE INTO bookings VALUES('%q',%lld,%d,%d,%llu,"
               "'HELD',%lld);",
               e.booking_id.c_str(),
               static_cast<long long>(e.user_id), e.seat_id, e.day_off,
               static_cast<unsigned long long>(e.mask),
               static_cast<long long>(e.ts_ms));
        else  // 拒绝也是事件：不落 bookings，留给审计流
          ;
        break;
      case EventType::CANCEL:
        set_state(e, "CANCELLED");
        break;
      case EventType::CHECKIN:
        set_state(e, "ACTIVE");
        break;
      case EventType::CHECKOUT:
        set_state(e, "DONE");
        if (e.released_mask == 0) credit(e, +1);  // 按时完成
        break;
      case EventType::NO_SHOW:
        set_state(e, "NO_SHOW");
        credit(e, -10);  // 爽约
        break;
      default:
        break;
    }
  }

  const char* name() const override { return "sqlite"; }

 private:
  void set_state(const Event& e, const char* st) {
    if (e.booking_id.empty()) return;
    exec("UPDATE bookings SET state='%q', updated_ms=%lld WHERE booking_id='%q';",
         st, static_cast<long long>(e.ts_ms), e.booking_id.c_str());
  }
  void credit(const Event& e, int delta) {
    exec("INSERT OR REPLACE INTO credit_events VALUES(%lld,%lld,%d,'%q');",
         static_cast<long long>(e.seq),
         static_cast<long long>(e.user_id), delta, e.booking_id.c_str());
  }
  void exec(const char* fmt, ...) {
    char* sql = nullptr;
    va_list ap;
    va_start(ap, fmt);
    sql = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
    char* err = nullptr;
    sqlite3_exec(db_, sql, nullptr, nullptr, &err);
    if (err) sqlite3_free(err);
    sqlite3_free(sql);
  }
  sqlite3* db_ = nullptr;
};

}  // namespace

IProjectionSink* make_sqlite_projection(const std::string& path) {
  return new SqliteProjection(path);
}

}  // namespace seatmesh
