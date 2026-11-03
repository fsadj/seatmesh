# 任务包（5 人版）

五个模块域任务包，每人领一包，覆盖三个开发阶段。领取前先读
[CONTRIBUTING.md](../CONTRIBUTING.md)（环境搭建、fork+PR 流程、契约变更流程）。

| 包 | 域 | 技术栈 |
|---|---|---|
| [T1 引擎](T1-引擎.md) | C++ 数据面：位图状态机、持久化、定时器、共识对接 | C++17 / 并发 / 文件 IO / ZMQ |
| [T2 共识与部署](T2-共识与部署.md) | Raft 共识库、failover、多校区部署编排 | C++ / Docker Compose / tc |
| [T3 网关与协议兼容](T3-网关与协议兼容.md) | HTTP API、读副本、WS、compat 同形端点 | Python / FastAPI / asyncio |
| [T4 控制面与前端](T4-控制面与前端.md) | 信用/冷却/禁约、Saga、页面与大屏 | Python / SQLite / HTML+JS / ECharts |
| [T5 质量智能与契约](T5-质量智能与契约.md) | 压测/审计/CI、一致性验证、预测/风控、契约管理 | Python / pytest / LightGBM / Prometheus |

规则速记：验收 = DoD 命令输出截图贴 PR；做不完交包内"保底"；
契约文件（`protos/`、`engine/include/engine/contracts.hpp`、`gateway/contracts.py`）
改动必须走契约变更流程（CONTRIBUTING §6）。
