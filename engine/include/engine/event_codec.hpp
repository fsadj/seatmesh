#pragma once
// Event 二进制编解码 + CRC32。FileEventLog 的记录 payload 与
// DbEventLog 的 events.payload 均使用本编码——两种存储因此字节级同构。
// 本编码仅服务课程/单机环境，不做跨字节序兼容（写明即契约）。
#include "engine/contracts.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace seatmesh {

// 表驱动 CRC32（IEEE 802.3 多项式），无第三方依赖
inline uint32_t crc32(const void* data, size_t n) {
  static uint32_t table[256];
  static bool init = [] {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k)
        c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      table[i] = c;
    }
    return true;
  }();
  (void)init;
  uint32_t c = 0xFFFFFFFFu;
  const uint8_t* p = static_cast<const uint8_t*>(data);
  for (size_t i = 0; i < n; ++i) c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
  return c ^ 0xFFFFFFFFu;
}

// 布局：
//   [0]      type u8
//   [1..8]   seq u64        [9..16] ts_ms i64   [17..24] user_id i64
//   [25..28] seat_id i32    [29..32] day_off i32
//   [33..36] start_min i32  [37..40] end_min i32
//   [41..48] mask u64       [49..56] released_mask u64
//   随后 5 个字符串（u16 长度 + 字节）：
//   request_id, booking_id, queue_ticket, trace_id, detail
inline std::string encode_event(const Event& e) {
  std::string buf;
  buf.reserve(128);
  auto put = [&buf](const void* p, size_t n) {
    buf.append(static_cast<const char*>(p), n);
  };
  const uint8_t t = static_cast<uint8_t>(e.type);
  put(&t, 1);
  put(&e.seq, 8); put(&e.ts_ms, 8); put(&e.user_id, 8);
  put(&e.seat_id, 4); put(&e.day_off, 4);
  put(&e.start_min, 4); put(&e.end_min, 4);
  put(&e.mask, 8); put(&e.released_mask, 8);
  for (const std::string* s : {&e.request_id, &e.booking_id, &e.queue_ticket,
                               &e.trace_id, &e.detail}) {
    const uint16_t n = static_cast<uint16_t>(s->size());
    put(&n, 2);
    put(s->data(), n);
  }
  return buf;
}

inline bool decode_event(const char* data, size_t n, Event& e) {
  if (n < 57) return false;
  size_t off = 0;
  auto get = [&](void* p, size_t k) {
    std::memcpy(p, data + off, k);
    off += k;
  };
  uint8_t t;
  get(&t, 1); e.type = static_cast<EventType>(t);
  get(&e.seq, 8); get(&e.ts_ms, 8); get(&e.user_id, 8);
  get(&e.seat_id, 4); get(&e.day_off, 4);
  get(&e.start_min, 4); get(&e.end_min, 4);
  get(&e.mask, 8); get(&e.released_mask, 8);
  for (std::string* s : {&e.request_id, &e.booking_id, &e.queue_ticket,
                         &e.trace_id, &e.detail}) {
    if (off + 2 > n) return false;
    uint16_t len;
    get(&len, 2);
    if (off + len > n) return false;
    s->assign(data + off, len);
    off += len;
  }
  return off == n;
}

}  // namespace seatmesh
