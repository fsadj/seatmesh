#pragma once
// 入站请求源适配器：stdin（本地调试）、回放文件（确定性仿真 DST）、ZMQ（正式）。
// 行协议：kind key=value ...，字段与 engine::Event 对应。
//   reserve user=1001 seat=3 day=0 start=120 end=240 rid=r1 trace=t001
//   cancel  booking=B1 | checkin booking=B1 | checkout booking=B1 | quit
#include "engine/contracts.hpp"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/select.h>
#include <unistd.h>
#endif

namespace seatmesh {

inline bool parse_line(const std::string& line, IncomingRequest& out) {
  std::istringstream iss(line);
  std::string kind;
  if (!(iss >> kind)) return false;
  out.kind = kind;
  out.ev = Event{};
  std::string tok;
  while (iss >> tok) {
    const auto eq = tok.find('=');
    if (eq == std::string::npos) continue;
    const std::string k = tok.substr(0, eq), v = tok.substr(eq + 1);
    if (k == "user") out.ev.user_id = std::stoll(v);
    else if (k == "seat") out.ev.seat_id = std::stoi(v);
    else if (k == "day") out.ev.day_off = std::stoi(v);
    else if (k == "start") out.ev.start_min = std::stoi(v);
    else if (k == "end") out.ev.end_min = std::stoi(v);
    else if (k == "rid") out.ev.request_id = v;
    else if (k == "trace") out.ev.trace_id = v;
    else if (k == "booking") out.ev.booking_id = v;
  }
  return true;
}

class StdinSource final : public IRequestSource {
 public:
  std::optional<IncomingRequest> poll() override {
#if defined(__unix__) || defined(__APPLE__)
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(STDIN_FILENO, &rfds);
    timeval tv{0, 0};
    if (!::select(STDIN_FILENO + 1, &rfds, nullptr, nullptr, &tv))
      return std::nullopt;
#endif
    std::string line;
    if (!std::getline(std::cin, line)) return std::nullopt;
    IncomingRequest req;
    return parse_line(line, req) ? std::optional(std::move(req)) : std::nullopt;
  }
};

class ReplaySource final : public IRequestSource {
 public:
  explicit ReplaySource(std::string path) : in_(path) {}
  bool exhausted() const override { return exhausted_; }
  std::optional<IncomingRequest> poll() override {
    std::string line;
    while (std::getline(in_, line)) {
      IncomingRequest req;
      if (parse_line(line, req)) return req;
    }
    exhausted_ = true;
    return std::nullopt;
  }

 private:
  std::ifstream in_;
  bool exhausted_ = false;
};

// TODO(M2/M4, W1)：ZMQ ROUTER 版本，帧 = [sender][payload]，
// 与网关 DEALER 池对接（protos/scheduler.proto 传输章节）。
class ZmqRouterSource final : public IRequestSource {
 public:
  std::optional<IncomingRequest> poll() override { return std::nullopt; }
};

}  // namespace seatmesh
