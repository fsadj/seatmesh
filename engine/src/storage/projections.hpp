#pragma once
// 投影实现：apply 之后的只读镜像。日志是事实，投影只是视图——
// 投影损坏可以随时从日志重建（审计器/对账也消费同一条事件流）。
#include "engine/contracts.hpp"

#include <cstdio>
#include <string>

namespace seatmesh {

class NullProjection final : public IProjectionSink {
 public:
  void on_apply(const Event&, uint64_t) override {}
  const char* name() const override { return "null"; }
};

// CSV 投影（文件操作交付物之二）：一行一事件，人可读、可被表格打开。
class CsvProjection final : public IProjectionSink {
 public:
  explicit CsvProjection(std::string path);
  ~CsvProjection() override;
  void on_apply(const Event& e, uint64_t index) override;
  const char* name() const override { return "csv"; }

 private:
  std::FILE* f_ = nullptr;
};

// SQLite 投影：bookings 可查询镜像 + credit_events 信用流水（定义在 .cpp）
IProjectionSink* make_sqlite_projection(const std::string& path);

}  // namespace seatmesh
