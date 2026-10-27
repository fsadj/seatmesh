#pragma once
// ITimerWheel 的 v1 实现：最小堆 + 惰性取消（~O(log n)）。
// v2 层级时间轮（docs/01 §3）是 M1 的冲刺项，实现同一接口即可替换——依赖反转。
#include "engine/contracts.hpp"

#include <algorithm>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace seatmesh {

class TimerHeap final : public ITimerWheel {
 public:
  uint64_t schedule(int64_t deadline_ms, std::string tag) override {
    const uint64_t id = next_id_++;
    heap_.push({deadline_ms, id});
    tags_.emplace(id, std::move(tag));
    return id;
  }

  void cancel(uint64_t id) override { cancelled_.insert(id); }

  std::vector<Timer> expire(int64_t now_ms) override {
    std::vector<Timer> due;
    while (!heap_.empty() && heap_.top().deadline <= now_ms) {
      const auto [dl, id] = heap_.top();
      heap_.pop();
      if (cancelled_.erase(id)) continue;
      Timer t;
      t.id = id;
      t.deadline_ms = dl;
      auto it = tags_.find(id);
      if (it != tags_.end()) {
        t.tag = it->second;
        tags_.erase(it);
      }
      due.push_back(t);
    }
    return due;
  }

 private:
  struct Node {
    int64_t deadline;
    uint64_t id;
    bool operator>(const Node& o) const { return deadline > o.deadline; }
  };
  std::priority_queue<Node, std::vector<Node>, std::greater<Node>> heap_;
  std::unordered_map<uint64_t, std::string> tags_;
  std::unordered_set<uint64_t> cancelled_;
  uint64_t next_id_ = 1;
};

}  // namespace seatmesh
