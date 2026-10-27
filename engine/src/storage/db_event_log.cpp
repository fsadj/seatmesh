#include "storage/db_event_log.hpp"

#include "engine/event_codec.hpp"

#include <sqlite3.h>

#include <stdexcept>

namespace seatmesh {

namespace {
void check(sqlite3* db, int rc, const char* what) {
  if (rc != SQLITE_OK)
    throw std::runtime_error(std::string(what) + ": " +
                             (db ? sqlite3_errmsg(db) : "no db"));
}
}  // namespace

DbEventLog::DbEventLog(std::string db_path) : db_path_(std::move(db_path)) {
  check(db_, sqlite3_open_v2(db_path_.c_str(), &db_,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                             nullptr),
        "open db");
  char* err = nullptr;
  if (sqlite3_exec(db_, "PRAGMA synchronous=FULL;"
                        "PRAGMA journal_mode=WAL;"
                        "CREATE TABLE IF NOT EXISTS events("
                        " idx INTEGER PRIMARY KEY AUTOINCREMENT,"
                        " type INTEGER NOT NULL,"
                        " ts_ms INTEGER NOT NULL,"
                        " trace_id TEXT NOT NULL,"
                        " payload BLOB NOT NULL,"
                        " crc INTEGER NOT NULL);"
                        "CREATE TABLE IF NOT EXISTS meta("
                        " k TEXT PRIMARY KEY, v BLOB);",
                  nullptr, nullptr, &err) != SQLITE_OK) {
    std::string msg = err ? err : "exec failed";
    sqlite3_free(err);
    throw std::runtime_error("schema: " + msg);
  }
  check(db_, sqlite3_prepare_v2(db_,
                                "INSERT INTO events(type, ts_ms, trace_id, "
                                "payload, crc) VALUES(?1,?2,?3,?4,?5);",
                                -1, &insert_, nullptr),
        "prepare insert");
}

DbEventLog::~DbEventLog() {
  if (insert_) sqlite3_finalize(insert_);
  if (db_) sqlite3_close(db_);
}

uint64_t DbEventLog::append(const Event& e) {
  const std::string payload = encode_event(e);
  const uint32_t crc = crc32(payload.data(), payload.size());
  sqlite3_reset(insert_);
  sqlite3_bind_int(insert_, 1, static_cast<int>(e.type));
  sqlite3_bind_int64(insert_, 2, e.ts_ms);
  sqlite3_bind_text(insert_, 3, e.trace_id.c_str(),
                    static_cast<int>(e.trace_id.size()), SQLITE_TRANSIENT);
  sqlite3_bind_blob(insert_, 4, payload.data(),
                    static_cast<int>(payload.size()), SQLITE_TRANSIENT);
  sqlite3_bind_int64(insert_, 5, crc);
  if (sqlite3_step(insert_) != SQLITE_DONE)
    check(db_, SQLITE_ERROR, "insert");
  sqlite3_reset(insert_);
  const uint64_t idx = static_cast<uint64_t>(
      sqlite3_last_insert_rowid(db_));
  idx_ = idx;
  if (apply_) apply_(e, idx);  // autocommit 已持久，契约满足
  return idx;
}

void DbEventLog::set_snapshot(SnapshotOut out, SnapshotIn in) {
  snap_out_ = std::move(out);
  snap_in_ = std::move(in);
}

bool DbEventLog::write_snapshot() {
  if (!snap_out_) return false;
  const std::string blob = snap_out_();
  sqlite3_stmt* st = nullptr;
  check(db_, sqlite3_prepare_v2(db_,
                                "INSERT OR REPLACE INTO meta(k,v) "
                                "VALUES('snap',?1),('snap_idx',?2);",
                                -1, &st, nullptr),
        "prepare snap");
  sqlite3_bind_blob(st, 1, blob.data(), static_cast<int>(blob.size()),
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(idx_));
  check(db_, sqlite3_step(st) == SQLITE_DONE ? SQLITE_OK : SQLITE_ERROR,
        "write snap");
  sqlite3_finalize(st);
  return true;
}

uint64_t DbEventLog::replay() {
  int64_t snap_idx = 0;
  {
    sqlite3_stmt* st = nullptr;
    check(db_, sqlite3_prepare_v2(db_,
                                  "SELECT v FROM meta WHERE k='snap_idx';",
                                  -1, &st, nullptr),
          "prepare read idx");
    if (sqlite3_step(st) == SQLITE_ROW)
      snap_idx = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
  }
  if (snap_idx > 0 && snap_in_) {
    sqlite3_stmt* st = nullptr;
    check(db_, sqlite3_prepare_v2(db_, "SELECT v FROM meta WHERE k='snap';",
                                  -1, &st, nullptr),
          "prepare read snap");
    if (sqlite3_step(st) == SQLITE_ROW) {
      const void* p = sqlite3_column_blob(st, 0);
      const int n = sqlite3_column_bytes(st, 0);
      snap_in_(std::string(static_cast<const char*>(p), static_cast<size_t>(n)));
    }
    sqlite3_finalize(st);
    idx_ = static_cast<uint64_t>(snap_idx);
  }

  sqlite3_stmt* st = nullptr;
  check(db_, sqlite3_prepare_v2(db_,
                                "SELECT idx, payload, crc FROM events "
                                "WHERE idx > ?1 ORDER BY idx;",
                                -1, &st, nullptr),
        "prepare replay");
  sqlite3_bind_int64(st, 1, snap_idx);
  Event e;
  for (;;) {
    const int rc = sqlite3_step(st);
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) check(db_, rc, "replay step");
    const uint64_t idx =
        static_cast<uint64_t>(sqlite3_column_int64(st, 0));
    const void* p = sqlite3_column_blob(st, 1);
    const int n = sqlite3_column_bytes(st, 1);
    const uint32_t crc =
        static_cast<uint32_t>(sqlite3_column_int64(st, 2));
    std::string payload(static_cast<const char*>(p), static_cast<size_t>(n));
    if (crc32(payload.data(), payload.size()) != crc) break;  // 损坏即停
    if (decode_event(payload.data(), payload.size(), e) && apply_)
      apply_(e, idx);
    idx_ = idx;
  }
  sqlite3_finalize(st);
  return idx_;
}

}  // namespace seatmesh
