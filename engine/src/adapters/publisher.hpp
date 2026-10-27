#pragma once
// 事件流发布适配器：InMemory（测试/单机演示）与 ZMQ PUB（正式拓扑）。
#include "engine/contracts.hpp"
#include "engine/event_codec.hpp"

#include <functional>
#include <string>
#include <vector>

#if defined(SEATMESH_HAS_ZMQ)
#include <zmq.hpp>
#endif

namespace seatmesh {

class InMemoryPublisher final : public IEventPublisher {
 public:
  using Sub = std::function<void(const Event&)>;
  void add_subscriber(Sub s) { subs_.push_back(std::move(s)); }
  void publish(const Event& e) override {
    for (auto& s : subs_) s(e);
  }

 private:
  std::vector<Sub> subs_;
};

#if defined(SEATMESH_HAS_ZMQ)
// 订阅端（网关/大屏/审计器）连接同一 endpoint 即可收到字节级事件。
class ZmqPublisher final : public IEventPublisher {
 public:
  explicit ZmqPublisher(const std::string& endpoint) : ctx_(), sock_(ctx_, zmq::socket_type::pub) {
    sock_.bind(endpoint);
  }
  void publish(const Event& e) override {
    const std::string payload = encode_event(e);
    zmq::message_t msg(payload.begin(), payload.end());
    (void)sock_.send(msg, zmq::send_flags::dontwait);
  }

 private:
  zmq::context_t ctx_;
  zmq::socket_t sock_;
};
#endif

inline std::unique_ptr<IEventPublisher> make_publisher(const std::string& spec) {
#if defined(SEATMESH_HAS_ZMQ)
  if (spec.rfind("tcp://", 0) == 0 || spec.rfind("ipc://", 0) == 0)
    return std::make_unique<ZmqPublisher>(spec);
#endif
  if (spec == "memory") return std::make_unique<InMemoryPublisher>();
  throw std::runtime_error("publisher spec 未编译支持: " + spec +
                           "（需要 cppzmq 并定义 SEATMESH_HAS_ZMQ）");
}

}  // namespace seatmesh
