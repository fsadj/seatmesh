#pragma once
// 入站请求源适配器：stdin（本地调试）、回放文件（确定性仿真 DST）、ZMQ（正式）。
// 行协议：kind key=value ...，字段与 engine::Event 对应。
//   reserve user=1001 seat=3 day=0 start=120 end=240 rid=r1 trace=t001
//   cancel  booking=B1 | checkin booking=B1 | checkout booking=B1 | quit
//   epoch 2.2（T1 包授权）：checkin leave=true = 中途离开；
//                          claim_ticket ticket=T1 = 候补授权认领。
#include "engine/contracts.hpp"

#include <cstdio>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/select.h>
#include <unistd.h>
#endif

#if defined(SEATMESH_HAS_ZMQ)
#include <zmq.hpp>
#endif

namespace seatmesh {

inline bool parse_line(const std::string& line, IncomingRequest& out) {
  std::istringstream iss(line);
  std::string kind;
  if (!(iss >> kind)) return false;
  out.kind = kind;
  out.ev = Event{};
  out.leave = false;
  out.enqueue_if_conflict = false;
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
    else if (k == "ticket") out.ev.queue_ticket = v;
    else if (k == "leave") out.leave = (v == "true" || v == "1");
    else if (k == "enqueue") out.enqueue_if_conflict = (v == "true" || v == "1");
  }
  return true;
}

// ---------------- JSON 帧 v0（与 T3 gateway/zmq_engine.py 对齐冻结） ----------------
// 请求 = {"kind":..., "request_id":..., "user_id":..., "seat_id":..., ...}（send_json）
// 应答 = {"ok":..., "booking_id":..., "conflict_mask":..., "detail":..., ...}
// 纯标准库实现，独立于 ZMQ 是否编译（转义/解析可无 cppzmq 单测）。

inline std::string json_escape(const std::string& s) {
  std::string o;
  o.reserve(s.size() + 8);
  char buf[8];
  for (char c : s) {
    switch (c) {
      case '"':  o += "\\\""; break;
      case '\\': o += "\\\\"; break;
      case '\n': o += "\\n"; break;
      case '\r': o += "\\r"; break;
      case '\t': o += "\\t"; break;
      case '\b': o += "\\b"; break;
      case '\f': o += "\\f"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          std::snprintf(buf, sizeof buf, "\\u%04x",
                        static_cast<unsigned char>(c));
          o += buf;
        } else {
          o += c;
        }
    }
  }
  return o;
}

// 定位 key 对应值的原始 span [b,e)（字符串含引号）；未找到返回 {npos, npos}。
inline std::pair<size_t, size_t> json_value_span(const std::string& j,
                                                 const char* key) {
  const std::string pat = "\"" + std::string(key) + "\"";
  size_t p = j.find(pat);
  while (p != std::string::npos) {
    const size_t q = j.find_first_not_of(" \t\r\n", p + pat.size());
    if (q != std::string::npos && j[q] == ':') {
      const size_t v = j.find_first_not_of(" \t\r\n", q + 1);
      if (v == std::string::npos) break;
      if (j[v] == '"') {                     // 字符串：扫描到未转义的闭引号
        size_t i = v + 1;
        while (i < j.size()) {
          if (j[i] == '\\') { i += 2; continue; }
          if (j[i] == '"') break;
          ++i;
        }
        return {v, std::min(i + 1, j.size())};
      }
      const size_t e = j.find_first_of(",}]", v);   // 数字/字面量
      return {v, e == std::string::npos ? j.size() : e};
    }
    p = j.find(pat, p + pat.size());
  }
  return {std::string::npos, std::string::npos};
}

inline std::string json_get_string(const std::string& j, const char* key,
                                   const std::string& def = "") {
  const auto [b, e] = json_value_span(j, key);
  if (b == std::string::npos || e - b < 2 || j[b] != '"') return def;
  std::string out;
  out.reserve(e - b);
  size_t i = b + 1;
  while (i + 1 < e) {                        // 逆扫描转义（完整转义还原）
    char c = j[i];
    if (c == '\\' && i + 2 < e) {
      const char n = j[i + 1];
      switch (n) {
        case 'n': out += '\n'; break;
        case 't': out += '\t'; break;
        case 'r': out += '\r'; break;
        case 'b': out += '\b'; break;
        case 'f': out += '\f'; break;
        case 'u': {
          if (i + 6 < e) {
            int cp = 0;
            for (int k = 0; k < 4; ++k) {
              const char h = j[i + 2 + k];
              const int d = (h >= '0' && h <= '9') ? h - '0'
                            : ((h | 32) >= 'a' && (h | 32) <= 'f')
                                  ? (h | 32) - 'a' + 10 : 0;
              cp = cp * 16 + d;
            }
            if (cp < 0x80) out += static_cast<char>(cp);
            else if (cp < 0x800) {
              out += static_cast<char>(0xC0 | (cp >> 6));
              out += static_cast<char>(0x80 | (cp & 0x3F));
            } else {
              out += static_cast<char>(0xE0 | (cp >> 12));
              out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
              out += static_cast<char>(0x80 | (cp & 0x3F));
            }
            i += 6;
            continue;
          }
          break;
        }
        default: out += n; break;
      }
      i += 2;
    } else {
      out += c;
      ++i;
    }
  }
  return out;
}

inline int64_t json_get_int(const std::string& j, const char* key,
                            int64_t def) {
  const auto [b, e] = json_value_span(j, key);
  if (b == std::string::npos) return def;
  const std::string tok = j.substr(b, e - b);
  char* endp = nullptr;
  const long long v = std::strtoll(tok.c_str(), &endp, 10);
  return (endp && endp != tok.c_str() && *endp == '\0')
             ? static_cast<int64_t>(v) : def;
}

inline bool json_get_bool(const std::string& j, const char* key, bool def) {
  const auto [b, e] = json_value_span(j, key);
  if (b == std::string::npos) return def;
  const std::string tok = j.substr(b, e - b);
  if (tok == "true") return true;
  if (tok == "false") return false;
  return def;
}

// JSON 帧 v0 → 入站请求（缺字段给中性默认，由引擎预检拒绝非法值）
inline bool parse_frame_v0(const std::string& payload, IncomingRequest& out) {
  out = IncomingRequest{};
  out.kind = json_get_string(payload, "kind");
  if (out.kind.empty()) return false;
  out.ev.request_id = json_get_string(payload, "request_id");
  out.ev.booking_id = json_get_string(payload, "booking_id");
  out.ev.trace_id = json_get_string(payload, "trace_id");
  out.ev.queue_ticket = json_get_string(payload, "ticket");
  out.ev.user_id = json_get_int(payload, "user_id", 0);
  out.ev.seat_id = static_cast<int32_t>(json_get_int(payload, "seat_id", -1));
  out.ev.day_off = static_cast<int32_t>(json_get_int(payload, "day_off", -1));
  out.ev.start_min =
      static_cast<int32_t>(json_get_int(payload, "start_min", -1));
  out.ev.end_min = static_cast<int32_t>(json_get_int(payload, "end_min", -1));
  out.leave = json_get_bool(payload, "leave", false);
  out.enqueue_if_conflict =
      json_get_bool(payload, "enqueue_if_conflict", false);
  return true;
}

inline std::string outcome_to_json(const Outcome& o) {
  std::string j = "{\"ok\":";
  j += o.ok ? "true" : "false";
  j += ",\"booking_id\":\"" + json_escape(o.booking_id) + "\"";
  j += ",\"conflict_mask\":" + std::to_string(o.conflict_mask);
  j += ",\"detail\":\"" + json_escape(o.detail) + "\"";
  j += ",\"applied_seq\":" + std::to_string(o.applied_seq);
  j += ",\"queue_ticket\":\"" + json_escape(o.queue_ticket) + "\"";
  j += ",\"queue_pos\":" + std::to_string(o.queue_pos);
  j += ",\"shed\":";
  j += o.shed ? "true" : "false";
  j += ",\"backoff_hint_ms\":" + std::to_string(o.backoff_hint_ms);
  j += "}";
  return j;
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

// ZMQ ROUTER 请求源：帧序 [identity][payload]，payload = JSON 帧 v0；
// poll() 非阻塞；应答经 send_reply 按 [identity][json] 回帧。
// 编译开关 SEATMESH_HAS_ZMQ（cppzmq 未必安装；默认构建用占位实现照常通过）。
#if defined(SEATMESH_HAS_ZMQ)
class ZmqRouterSource final : public IRequestSource {
 public:
  static constexpr bool available() { return true; }
  explicit ZmqRouterSource(const std::string& endpoint)
      : ctx_(), sock_(ctx_, zmq::socket_type::router) {
    sock_.set(zmq::sockopt::linger, 0);
    sock_.bind(endpoint);   // DEALER（T3 网关池）→ ROUTER（本引擎）
  }

  std::optional<IncomingRequest> poll() override {
    IncomingRequest req;
    std::string identity;
    if (!pop_request(req, identity)) return std::nullopt;
    return req;
  }

  // 取一条请求并带出路由 identity（应答必须按原 identity 回帧）
  bool pop_request(IncomingRequest& out, std::string& identity) {
    drain();
    if (pending_.empty()) return false;
    identity = std::move(pending_.front().first);
    out = std::move(pending_.front().second);
    pending_.pop_front();
    return true;
  }

  // 应答帧序 [identity][json]
  bool send_reply(const std::string& identity, const std::string& json) {
    try {
      sock_.send(zmq::buffer(identity), zmq::send_flags::sndmore);
      sock_.send(zmq::buffer(json), zmq::send_flags::none);
      return true;
    } catch (const zmq::error_t&) {
      return false;
    }
  }

 private:
  void drain() {   // 非阻塞收满缓冲（ROUTER：先 identity 帧，再 payload 帧）
    for (;;) {
      zmq::message_t id_msg, pay_msg;
      if (!sock_.recv(id_msg, zmq::recv_flags::dontwait)) return;
      if (!sock_.recv(pay_msg, zmq::recv_flags::dontwait)) return;
      IncomingRequest req;
      if (parse_frame_v0(
              std::string(static_cast<const char*>(pay_msg.data()),
                          pay_msg.size()),
              req))
        pending_.emplace_back(
            std::string(static_cast<const char*>(id_msg.data()),
                        id_msg.size()),
            std::move(req));
    }
  }

  zmq::context_t ctx_;
  zmq::socket_t sock_;
  std::deque<std::pair<std::string, IncomingRequest>> pending_;
};
#else
// 未定义 SEATMESH_HAS_ZMQ：cppzmq 未必安装，占位实现保证默认构建照常通过。
class ZmqRouterSource final : public IRequestSource {
 public:
  static constexpr bool available() { return false; }
  explicit ZmqRouterSource(const std::string& = std::string()) {}
  std::optional<IncomingRequest> poll() override { return std::nullopt; }
  bool pop_request(IncomingRequest&, std::string&) { return false; }
  bool send_reply(const std::string&, const std::string&) { return false; }
};
#endif

}  // namespace seatmesh
