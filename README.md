# SeatMesh

多校区座位资源调度平台：C++ 无锁位图状态机数据面 + Python FastAPI 网关与控制面。

## 核心设计

- **一个座位一天 = 一个 `uint64`**：营业时段 06:00–22:00 按 15 分钟切片，恰好 64 片；
  冲突检测是一次位运算，抢座是一次原子 CAS——成功即线性化点，并发下绝不超卖。
- **日志即数据库**：预约、管理指令、配置变更全部走同一条追加日志；
  文件（WAL）与 SQLite 是同一契约的两套持久化实现，启动参数一键切换。
- **依赖反转**：引擎核心只依赖抽象接口（`contracts.hpp`），
  文件/数据库/ZMQ/内存实现全部在边缘装配；上层逻辑零改动可换底层。
- **读写分离**：引擎只承受写流量；查询由网关从事件流维护的本地位图副本直接应答。
- **语义对齐**：业务规则与预约单状态机与主流校园图书馆选座系统一致
  （入馆资格 30min、中途离开保留期、取消冷却等）；为**平行实现**，
  不连接、不依赖任何真实系统。

## 快速开始

```bash
make build-engine    # 编译引擎（依赖 sqlite3）
make test-engine     # 契约一致性测试：文件/数据库/内存三实现字节级一致
make demo-engine     # 回放脚本演示：预约/冲突/取消 + CSV 投影
make demo-python     # Python 契约演示：100 线程并发抢 1 座恰好 1 人成功
```

引擎直接运行：

```bash
engine_d --storage=file   --source=replay --replay=demo.txt --projection=csv
engine_d --storage=sqlite --source=stdin
```

## 目录结构

```
protos/    传输层契约（scheduler.proto）
engine/    C++ 数据面：contracts 契约 / 位图状态机 / 文件与数据库日志 / 投影
gateway/   Python：contracts 契约 / Mock 引擎 / ZMQ 客户端 / 信用分存储
```

## 契约

三层契约互为镜像，修改需走评审流程：

- `protos/scheduler.proto` —— 消息、错误码、全局常量
- `engine/include/engine/contracts.hpp` —— C++ 抽象接口
- `gateway/contracts.py` —— Python 抽象接口

## 参与开发

组员请先读 [CONTRIBUTING.md](CONTRIBUTING.md)（开发手册：环境搭建、fork+PR 流程、
分支规范、契约变更流程），再领取各自的任务包。`main` 分支受保护，只接受 PR 合并。

## License

MIT
