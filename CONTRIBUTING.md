# SeatMesh 开发手册

> 所有组员开工前必须完整读完本文档。读完的标准：你能独立完成
> 「同步 → 建分支 → 开发 → 本地验证 → PR」一个完整循环。

## 1. 项目一页纸

**是什么**：多校区座位资源调度平台——C++ 无锁位图状态机数据面，Python FastAPI 网关与控制面。

**三条设计支柱**（所有代码决策向上对齐）：

1. **一个座位一天 = 一个 `uint64`**：营业时段 06:00–22:00 按 15 分钟切片，
   每天 64 片。冲突检测 = 一次位运算；抢座 = 一次原子 CAS，成功即线性化点。
2. **日志即数据库**：预约、管理指令、配置变更全部走同一条追加日志；
   文件（WAL）与 SQLite 是同一契约的两套实现，启动参数切换。
3. **读写分离**：引擎只承受写流量；查询由网关本地副本直接应答。

**目录结构**：

```
protos/    传输层契约 scheduler.proto（消息/错误码/全局常量）
engine/    C++ 数据面
  include/engine/contracts.hpp   ← C++ 抽象接口（契约）
  src/engine.cpp                 ← 状态机参考实现
  src/storage/                   ← 文件日志 / SQLite 日志 / 投影
  src/adapters/                  ← ZMQ / stdin / 内存 适配器
gateway/   Python 控制面
  contracts.py                   ← Python 抽象接口（契约）
  mock_engine.py                 ← Mock 引擎（语义与 C++ 一致）
  stores.py                      ← 信用分存储（内存/SQLite）
Makefile   build-engine / test-engine / demo-engine / demo-python
```

**三层契约互为镜像**：`scheduler.proto` ↔ `contracts.hpp` ↔ `contracts.py`。
常量以 proto 头部注释为准（签到窗 15min、授权期 60s、单次 ≤16 片……），
**代码里不得另立常量**。

## 2. 一次性环境准备

1. 注册 GitHub 账号，把 `github.com/你的用户名` 发到群里。
2. 打开 `https://github.com/fsadj/seatmesh`，点右上角 **Fork**。
3. 克隆**你自己的 fork**（注意 URL 换成你的用户名）：
   ```bash
   git clone https://github.com/你的用户名/seatmesh.git
   cd seatmesh
   git remote add upstream https://github.com/fsadj/seatmesh.git
   ```
   `origin` = 你的 fork（你有写权限）；`upstream` = 主仓库（只读）。
4. 工具链：
   ```bash
   # C++：clang++ 或 g++（macOS 装 Xcode Command Line Tools 即可）
   # SQLite：macOS 自带；Linux：sudo apt install libsqlite3-dev
   # Python ≥3.10：
   python3 -m venv .venv && source .venv/bin/activate
   pip install fastapi uvicorn httpx pytest
   ```
5. 验证环境（三条全绿才算就绪）：
   ```bash
   make test-engine    # 契约一致性测试，8/8 PASS
   make demo-engine    # 回放演示，输出预约/冲突/取消
   make demo-python    # 100 线程抢 1 座恰好 1 人成功
   ```

## 3. 分支与提交规范

- `main` 受保护：**只接受 PR 合并**，禁止直接 push（即使有权限也请走 PR）。
- 分支命名：`p<阶段>-m<席位>-<主题>`，例如 `p1-m4-auth`、`p2-m1-grant-queue`。
- 一个分支只做一件事；做完即发 PR，不留长期分支。
- 提交信息：第一行 ≤50 字说清做了什么，正文补充动机：
  ```
  fix(engine): checkout 释放计算按营业日锚点取整

  之前用 UTC 当日 0 点推算 consumed slots，跨日预约会多算一天。
  ```

## 4. 日常开发循环（每天的第一件事和最后一件事）

```bash
# ① 同步主仓库（每天开工先做，防止在旧代码上开发）
git checkout main
git fetch upstream
git merge --ff-only upstream/main
git push origin main          # 同步你自己的 fork

# ② 建任务分支
git checkout -b p1-m4-auth

# ③ 开发，小步提交
git add -p                    # 逐块确认，不要 git add .
git commit -m "..."

# ④ 提交前本地验证（对应你改动范围的全部命令）
make test-engine && make demo-python

# ⑤ 推送并发 PR
git push origin p1-m4-auth
# 打开 GitHub，点 Compare & pull request
```

**PR 必须包含**：

- 标题：`[P1-M4] 网关鉴权与限流`（阶段-席位-主题）；
- 描述：做了什么、为什么、对应任务包编号；
- **验收截图**：DoD 命令的输出贴在 PR 描述里——任务包规则，没有截图不予评审；
- 关联 issue（如有）。

**评审与合并**：任务包指定的评审人（或目录 owner）逐条对照 DoD 敲命令；
通过 → Squash merge；不通过 → 在 PR 里写明原因，作者修改后重新请求评审。
**评审意见请全部回应**（改了或说明为什么不改），不要静默。

## 5. 任务包工作法

- 每人手里有 3 个任务包（阶段一/二/三），先读自己分册的「需求」编号清单；
- **DoD 是唯一完成标准**：不看"我觉得做完了"，看命令输出；
- 做不完整个包 → 交付**保底版**（每包末尾有定义），在 PR 标题注明 `[保底]`，
  如实写进迭代报告——保底不是失败，虚报才是；
- 依赖别人的接口未就绪 → 使用 Mock/骨架先开发，**不要等**；
  确实被阻塞 → 周五站会前在群里发"卡点：谁-什么-需要什么"。

## 6. 契约变更流程（最重要的一节）

以下文件是契约：`protos/scheduler.proto`、`engine/include/engine/contracts.hpp`、
`gateway/contracts.py`，以及双方已签字的 JSON 帧格式 / HTTP API 表。

**想改契约**（加字段、改语义、改常量）：

1. 开 issue 描述动机与方案；
2. 契约管理员（M9）汇总，组织双端评审（引擎侧 M1 + 网关侧 M4）；
3. 双端签字后：field 编号只增不删，更新三层契约与 `epoch+1`；
4. 契约测试（`make test-engine` + `make demo-python`）回归通过方可合并。

**禁止**：私下改契约文件、口头约定接口、"顺手"改了共享结构体。
契约测试红 = 全组停手修契约，其他 PR 一律不合。

## 7. CI 红了怎么办

1. 打开 PR 的 Actions 页，看**哪一个 job** 红（引擎契约测试 / Python 演示 / 压测）；
2. 本地跑同一条命令复现；
3. 修复后 push，CI 自动重跑；
4. **禁止**无改动重复重跑（"再跑一次说不定就好了"不是工程方法）；
5. 首次从 fork 发 PR 时，CI 需要维护者点一次 "Approve and run"——群里喊一声即可。

## 8. 代码规范

- **C++**：C++17；命名 `snake_case`（函数/变量）、`PascalCase`（类型）、
  `k` 前缀常量；核心代码（`include/engine/`）**禁止引入新依赖**（zmq/sqlite 只许出现在
  `src/storage`、`src/adapters`）；每个 public 接口带注释说明语义与边界。
- **Python**：类型标注（公开函数必须有）；标准库优先，新增第三方依赖需在 PR 里说明理由
  并更新 `requirements.txt`；模块头注释写明职责与消费方。
- **目录所有权**：`engine/` 评审人 M1/M2/M3 之一；`gateway/` 评审人 M4/M5；
  契约文件评审人 M9 + 双端各一人。
- 提交前自查：没有调试 print、没有注释掉的死代码、没有与本次任务无关的改动混入。

## 9. 沟通与节奏

- **周五站会（30 分钟）**：每人 3 分钟——上周完成 / 本周计划 / 卡点。
  卡点格式："谁-什么-需要什么"（例：M6-M4-座位 API 的 stale 字段还没合）。
- **迭代验收会**（每阶段末）：现场逐条打勾 AC，重跑前序阶段全部 AC。
- 找人：任务包分册末尾的「依赖关系」就是找谁的正确姿势。

## 10. FAQ

**Q：fork 落后主仓库很多了？**
按 §4 ① 的三条命令同步；有本地分支冲突时先 `git rebase upstream/main` 解决。

**Q：合并冲突怎么解？**
`git checkout p1-m4-auth && git rebase upstream/main`，逐文件解决后
`git rebase --continue`；解决不了带着冲突文件找目录 owner，10 分钟内能理清。

**Q：我改的文件被谁评审？**
看 §8 目录所有权；拿不准就在 PR 里 `@` 契约管理员（M9）转发。

**Q：能不能直接改 main？**
不能。main 受保护是全组的刹车，不是流程官僚。

**Q：任务包接口规范和代码对不上？**
以契约文件为准，立刻给 M9 开 issue——这正是契约测试要抓的不一致。

## 11. 命令速查

| 目的 | 命令 |
|---|---|
| 编译引擎 | `make build-engine` |
| 全部契约测试 | `make test-engine` |
| 引擎回放演示 | `make demo-engine` |
| Python 契约演示 | `make demo-python` |
| 同步主仓库 | `git fetch upstream && git merge --ff-only upstream/main` |
| 起网关（Mock） | `uvicorn app:create_app --factory`（M4 合入后） |
