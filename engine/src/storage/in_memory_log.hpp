#pragma once
// 内存日志：单测与契约一致性测试用。生产路径请用 File/Db 实现。
#include "engine/contracts.hpp"
#include "engine/event_codec.hpp"

#include <string>
#include <vector>

namespace seatmesh {

class InMemoryEventLog final : public IEventLog {
 public:
  uint64_t append(const Event& e) override {
    store_.push_back(encode_event(e));
    const uint64_t idx = store_.size();
    if (apply_) apply_(e, idx);
    return idx;
  }
  void barrier() override {}
  void set_apply(ApplyFn fn) override { apply_ = std::move(fn); }
  bool write_snapshot() override {
    if (!snap_out_) return false;
    snap_ = snap_out_();
    snap_idx_ = store_.size();
    return true;
  }
  uint64_t replay() override {
    if (!snap_.empty() && snap_in_) snap_in_(snap_);
    Event e;
    for (size_t i = snap_idx_; i < store_.size(); ++i) {
      if (decode_event(store_[i].data(), store_[i].size(), e) && apply_)
        apply_(e, i + 1);
    }
    return store_.size();
  }
  uint64_t applied_index() const override { return store_.size(); }

 private:
  std::vector<std::string> store_;
  ApplyFn apply_;
  SnapshotOut snap_out_;
  SnapshotIn snap_in_;
  std::string snap_;
  size_t snap_idx_ = 0;
};

}  // namespace seatmesh
