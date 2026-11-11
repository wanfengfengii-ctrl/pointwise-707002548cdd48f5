# 采集器重构后的职责与依赖说明

本次重构把原先同时包含状态机、SQL 和传输编排的 `harvest/legacy.py` 拆成
四层，业务流程可以在完全没有数据库的情况下验证。依赖方向单向向内，
没有任何反向依赖或旁路执行器。

## 模块职责

```
replay / __main__ / checks.py        装配层（场景、录制替身、CLI）
        │
        ▼
legacy.Collector                     兼容外观：仅构造并转发，无 SQL/状态机
        │
        ▼
application.Runner                   应用编排：端口调用顺序、事务、故障点、步数预算
        │
        ├──▶ domain（纯函数核心）     页校验/摘要、phase 转移、响应决策、版本合并
        │
        ├──▶ ports.Portal            远端边界 request(dict)
        │
        └──▶ ports.Repository        持久化最小端口（9 个方法）
                    ▲
                    │ 实现
        adapters/sqlite.SqliteRepository
        verify_structure.InMemoryRepository（验收替身，只实现端口）
```

### `harvest/domain.py` —— 无 I/O 领域核心

- 输入只接受普通 JSON 值（dict/list/str/int/bool/None），返回普通值、
  声明式 effect 描述符（`{"kind": ..., "control": ..., "events": ...}`）
  或抛 `HarvestError`。
- 负责：页形状与严格类型校验、去 token 的 canonical SHA-256 摘要、
  初始 control、`plan_step` 的 phase 决策、login/receipt/missing/expire
  等响应决策、`merge_decision` 的 rev 版本合并与 `REVISION_CONFLICT`。
- 不 import 仓储/适配器/sqlite3/os/sys，不接收连接、repository、portal
  或回调，不读时钟/随机数，不用 `global`；所有输出经 `copy.deepcopy`
  与调用方输入隔离，不修改入参。

### `harvest/ports.py` —— 结构化接口

- `HarvestError`、`Portal`、`Repository`（`typing.Protocol`，
  `@runtime_checkable`）。不绑定任何具体适配器。
- Repository 固定九个端口：`read_control/read_record/read_receipt/
  write_control/write_record/write_receipt/append_audit/transaction/
  snapshot`，应用层不使用端口之外的任何能力。

### `harvest/application.py` —— Runner 编排

- `Runner(repository, portal)` 提供兼容的 `run(steps, fault)`、`state()`、
  `snapshot()`、`page(raw, cursor)`。
- 只做：按 `domain.plan_step` 组织端口调用、在事务内执行领域 effect、
  保留四个历史故障注入点（`after_fetch/after_stage/after_ack/in_commit`）
  与旧循环步数预算（迭代预算而非请求数，`steps=0` 无请求，done 返回 0）。
- 不决定 phase、不自行校验门户字段、不合并版本；不 import
  legacy/replay/adapters/sqlite3。

### `harvest/adapters/sqlite.py` —— SQLite 适配器

- 只负责四表 schema（与旧版逐字相同的 `CREATE TABLE IF NOT EXISTS`）、
  SQL、DTO 编解码（`deleted` 0/1 ↔ bool、value/control/audit 的 canonical
  JSON 文本）、`BEGIN IMMEDIATE/COMMIT/ROLLBACK` 原子事务。
- 空连接初始化四表与初始 control；已有连接内容逐字保留，不做迁移、
  补写或清空。不决定采集流程，不调用 portal。
- 事务不支持嵌套；事务内写入可被同事务读到。

### `harvest/legacy.py` —— 兼容外观

- 保留 `Collector(connection, transport)` 与 `HarvestError` 的旧入口
  （`HarvestError` 经 `ports` 重新导出）。
- 内部仅构造 `SqliteRepository` 与 `Runner` 并转发 `run/state/snapshot/
  page`；没有 SQL、phase 状态机或备用旧执行器，所有旧入口都走同一套
  Runner + domain + 适配器。

### `harvest/replay.py` / `__main__.py` —— 装配与回放

- `ScriptedPortal` 仍是确定性录制替身；`replay` 负责每次 run 前用
  SQL dump 重开连接，直接装配 `SqliteRepository + Runner`（不再经
  legacy），业务失败捕获为 `{"error": CODE}`，录制不一致的
  `AssertionError` 不被吞掉。

## 兼容行为要点（未"纠正"历史边界）

- 摘要只覆盖 `page/cursor/next/rows`，行顺序参与，token 不参与；
  refresh 必须同摘要，只允许换 token。
- 远端确认成功但本地无 receipt 时，先按旧检查点以 `status` 查证；
  只有 `{"missing": true}` 才回到 staged 并以同摘要重新 fetch 绑定
  新 token。
- 会话可在任一请求过期：过期在独立事务里清 session 并审计
  `expired`，当前迭代即结束，下一次迭代重新 login，pending 工作不丢。
- null 响应消耗一步但不过期 session（login 无此豁免）；故障优先级：
  `after_fetch` 崩溃优先于 refresh 摘要不一致的 `PAGE_CHANGED`。
- 提交在单事务内完成：整页 rows + receipt + cursor/control + audit
  要么全部可见要么全部回滚；已持久化的 receipted 状态不回退。
- 四表 schema、control JSON、audit 事件名/字段/顺序、故障位置与调用
  次数均与旧实现一致，`samples/expected.json` 校验和不变。

## 验证入口

- `python3 checks.py normal|verify|demo`：原有回归与恢复演示，内容未改。
- `python3 verify_structure.py`：新增结构验证（AST 依赖方向、内存替身
  对等回放且禁用 sqlite3、领域纯度与确定性、磁盘 SQLite 跨连接恢复及
  旧 schema 验收）。
- `python3 verify_all.py`：单入口，先跑全部旧回归再跑结构验证；
  `.pointwise-runtime.json` 的 `verify` 指向它，`demo` 仍指向
  `checks.py demo`（输出每次 run 的 phase/请求数、最终四表快照与完整
  远端 trace）。
