# 采集器的兼容及边界契约

这是公开资料门户抓取作业的离线协议回放项目，不是通用命令行工具。
门户通过 fetch 返回一页记录；ack 会推进门户侧的交付状态。
响应丢失不能证明 ack 没有发生。status 根据 page + digest 查询确认结果；
只有 missing 后才能重新抓取绑定新 token。会话可在任一请求过期。
ScriptedPortal 是线上传输边界的确定性录制替身，不需要账号、网络或浏览器。

## 已有入口与格式

Python 3.9+，仅标准库。仓库根目录：
- python3 -m harvest samples/normal.json
- python3 -m harvest samples/recovery.json
- python3 checks.py normal
- python3 checks.py verify
- python3 checks.py demo

模块入口读取一个 UTF-8 JSON 场景，输出一个 JSON 对象。
actions 是依次消费的远端交互录制：
{"expect": REQUEST, "return": RESPONSE} 或 {"expect": REQUEST, "raise": ERROR_CODE}。
runs 是 Collector.run 的关键字参数列表，每次调用前用 SQLite SQL dump 关闭并重建连接，
以检验持久化检查点恢复；远端录制位置不回退。
输出 runs 每项有 result、snapshot、calls，trace 是完整远端请求序列。
场景内业务失败由 replay 捕获为 {"error": CODE}，整个回放成功时进程退出 0；
交互与录制不一致是验证失败，不可吞掉 AssertionError。
JSON 对象键顺序、输出空白不参与兼容；数组顺序、数字/布尔类型参与。

保留 harvest.legacy.Collector(connection, transport) 和 HarvestError，
Collector.run(steps=100, fault=None)、state()、snapshot()、page(raw,cursor)。
transport.request(request_dict) 返回 JSON 值或抛 HarvestError。
run 的 steps 是旧循环迭代预算，不是 HTTP 请求数；
steps=0 不产生请求，作业已经 done 则返回 done/0。
page 返回深拷贝后的页与 SHA-256 摘要，仍会查已提交 page 的摘要冲突。
不要顺便收紧原实现的响应判断或“纠正”历史边界行为。

fetch 页形状见 samples：page/cursor/next/token/rows；
row 固定为 id/rev/deleted/value，rev 拒绝 bool，value 为对象。
摘要是去掉 token 后页的 canonical UTF-8 JSON 的 SHA-256：
json.dumps(sort_keys=True, ensure_ascii=False, separators=(",",":"), allow_nan=False)。
行顺序参与摘要；token 不参与。refresh 必须保持同一摘要。
记录合并按页内顺序：更高 rev 替换，较低忽略，相等同内容为空操作、
相等异内容 REVISION_CONFLICT；墓碑仍保留 value 并参与同版本比较。
提交失败时全页数据、receipt、cursor、audit 一并回滚，已持久化的 receipted 状态不回退。

四张表的 SQL schema、字段内容、audit 顺序和 control JSON 格式均属于兼容面。
允许内部新增模块，不能迁移、补写、修正或清空旧检查点。
fault 支持 after_fetch、after_stage、after_ack、in_commit；
故障位置和旧循环中的调用次数、事务可见状态均需保留。
正常样例第一次 done/7，第二次 done/0；最终 a 为 rev2 墓碑，
b 保留 rev2 的“乙”，c 为 rev0 的“丙”。恢复样例最终 session 为 S4，
远端共 18 次请求，审计共 21 条。expected.json 固定整个回放的规范摘要，
checks.py 另外断言中间检查点，不允许重新录制期望值来适配重构。

## 本次要形成的结构边界

下列路径/入口是交付契约，内部事件和效果的数据设计由实现者决定。
不预先提供拆分后的实现。
- harvest/domain.py：纯业务核心。页校验/摘要、phase 转移、响应及错误决策、
  记录版本合并均在这里。输入只能是普通值，返回普通值/声明式操作或业务异常；
  不执行传输/仓储调用，不接收连接、repository、portal 或可执行 I/O 回调，
  不修改调用方输入，不读时钟/随机数/全局可变状态。
- harvest/ports.py：HarvestError、Repository 与 Portal 的结构化接口；
  可用 typing.Protocol 或抽象类，不绑定具体适配器。
- harvest/application.py：Runner(repository, portal)，提供兼容的 run/state/snapshot/page。
  只组织端口调用、执行领域产生的操作及事务/故障注入，不决定业务 phase、
  不自行校验门户字段或合并版本，不依赖 legacy/replay/adapters/sqlite3/文件和网络。
- harvest/adapters/sqlite.py：SqliteRepository(connection)，负责 schema、SQL、
  原子提交/回滚、数据 DTO 编解码，不决定采集流程、不调用 portal。
- harvest/legacy.py：兼容外观，负责把旧构造参数接到上述组件并转发旧方法；
  不留 SQL、状态机或备用旧执行器。replay/CLI 可承担装配职责。
  所有旧入口的真实执行必须经过同一 Runner、领域核心及仓储适配器。

Repository 固定最小端口如下，便于独立验收注入无 SQLite 的实现：
read_control() -> dict
read_record(id) -> dict | None
read_receipt(page) -> dict | None
write_control(control) -> None
write_record(row) -> None
write_receipt(receipt) -> None
append_audit(event) -> None
transaction() -> 上下文管理器
snapshot() -> 与旧 snapshot 相同的 dict

row DTO 与 snapshot.records 每项相同；receipt DTO 与 snapshot.receipts 每项相同。
仓储读写按值隔离，不得让未提交的输入 dict 修改已存状态。
transaction 在进入时建立事务，正常退出提交，任意 BaseException 时回滚；
事务内更新可被同事务读取，不能覆盖 portal 调用；不支持嵌套事务。
SqliteRepository 初始化空连接的四表及初始 control，已有连接逐字保留内容。
应用不调用端口之外的仓储属性、SQL、dump、连接或反射能力，
snapshot 仅用于公开读取，不能拿全库快照驱动业务判断。
Portal 仍是 request(dict)；领域不依赖它。
旧 Collector.page 的 receipt 查询通过 Runner/Repository 完成。

## 交付验证

checks.py 与 samples 的现有内容不可删改或削弱，可新增 checks 之外的测试。
增加结构验证脚本并把 runtime.verify 指向“已有全部回归 + 新结构验证”的单入口。
结构验证必须实际运行，而不仅检查文件存在：
1. AST/导入关系证明以上依赖方向，无动态导入、eval/exec 或转发到遗留逻辑逃逸。
2. 独立的内存 Repository 替身只实现以上端口、支持事务回滚。
   用 Runner + 此替身回放 normal、recovery 和冲突场景，
   结果、每次调用后的持久值与远端 trace 和 SQLite 路径一致；
   禁用 sqlite3.connect 和 SqliteRepository 构造仍能完成。
3. 核心对同一输入重复求值相等，输入深拷贝前后相等；
   覆盖正常、未知确认、会话过期、refresh 摘要改变、提交版本冲突。
4. 用实际磁盘 SQLite 文件跨连接重开：至少在 staged、uncertain、
   receipted 及 in_commit 回滚后接续。独立验收将创建旧版 schema/control，
   不会先运行新版本“准备迁移”。不得依赖重放器在进程内保存业务状态。

保留 .pointwise-runtime.json，version=1，kind=cli，setup 为 argv 列表，
verify/demo 为直接 argv；demo 展示实际恢复轨迹和最终行，不只打印成功标记。
可在 macOS 本机用 Python 运行，无 Docker、服务、账号、截图或录屏要求。
