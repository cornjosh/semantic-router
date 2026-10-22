# 共享负载与在线学习

`semantic_router.adaptive.state.postgres.PostgresState` 使用 PostgreSQL 保存决策、幂等键、观测、质量反馈和轮询租约。多个服务副本使用相同的数据库与 schema，无需 sticky session。该模块已经通过真实数据库和独立 Python 子进程测试；完整 HTTP 服务和 Higress 转发链路仍需分别验证。

## 本机复现

安装 PostgreSQL 服务端工具（测试机器使用 Ubuntu 的 PostgreSQL 14.24），然后从仓库根目录执行：

```bash
bash scripts/adaptive/local-postgres.sh start
export ADAPTIVE_TEST_DSN=postgresql://postgres@127.0.0.1:15432/postgres
uv run --extra postgres pytest tests/integration/adaptive/test_shared_state.py -q
uv run pytest tests/unit/adaptive/test_selection.py -q
bash scripts/adaptive/local-postgres.sh status
# 本次测试结束后，可停止专用实例；不删除数据。
bash scripts/adaptive/local-postgres.sh stop
```

脚本只监听 `127.0.0.1`，使用 trust 认证，仅供本机测试。以 root 执行时用系统 `postgres` 用户运行数据库；普通用户可自行初始化。数据默认在 `/tmp/semantic-router-adaptive-pg`，不是生产持久卷。可设置 `PG_BIN`、`ADAPTIVE_PG_PORT` 和 `ADAPTIVE_PG_ROOT`；后者必须是绝对路径，目录名以 `semantic-router-` 开头。使用已有 compose 测试数据库时，将 DSN 改为 `postgresql://postgres:postgres@127.0.0.1:5432/postgres` 即可。每个测试创建唯一 schema，结束后清理自己创建的 schema，不删除其他测试或业务数据。

## 决策、重放与观测

服务将完整 `Decision` 写入数据库后才能返回。`(tenant_id, request_id)` 唯一约束和请求 fingerprint 保证并发重放返回同一条已提交决策；同 ID 不同内容返回冲突，不覆盖旧决策。决策记录不包含原始 prompt，保留模型版本、候选模型、实际选择概率、上下文桶和信号状态。决策 ID 也不能重用于另一请求。

一次决策对应一个逻辑转发 attempt。`Observation` 必须匹配决策中的租户、attempt 和集群。相同事件 ID、相同内容可重复提交；同事件 ID 改变内容会冲突。不同事件 ID 的重复 end/reset 回调以第一条终态为准，只更新一次。记录的 outstanding 是尚未结束且未到 `expires_at` 的决策数：重启不会清零，丢失终态会在有限时间后退出估计。它不保证精确的物理请求数，也不是容量预留。网关内部的物理重试应作为同一个逻辑 attempt 的耗时处理。

成功响应的延迟、可用的 TTFT、运行失败率分别更新 EWMA。timeout/upstream error 影响失败率与连续失败冷却；client cancel 只结束 outstanding，不被计为后端成功或能力失败。TTFT 未知保留 `None`，不补零。TTFT 与总延迟各自有时间戳；新非流式响应不会刷新旧 TTFT。过期的历史值在出现新样本时重新初始化，乱序/过期观测会结束对应 outstanding，但不会改写当前 EWMA。`samples` 表示实际更新 EWMA 的样本数，而非网关总请求数。

数据库时间用于租约、冷却和 outstanding 截止判断。观测时间允许 5 秒时钟偏差，超过该范围的未来观测或明显早于决策的观测会拒绝。部署时需保证网关、服务与数据库时钟同步。

## 模型与集群分别选择

`choose_model()` 只接收已通过硬约束的候选；它不能扩展候选集合。每个 `(tenant, policy_version, feature_version, category, difficulty, model_id, model_version)` 有独立的成功/失败计数。Beta 先验由 `ModelArm.prior_success` / `prior_failure` 明确配置，默认均为 1；后验均值为 `(prior_success + successes) / (prior_success + prior_failure + successes + failures)`。

默认 `epsilon=0`，选后验均值最高的候选，同分按模型 ID 排序。启用 epsilon-greedy 后，以 epsilon 的概率均匀探索合格候选；最佳模型的实际选择概率是 `1-epsilon+epsilon/N`，其他候选是 `epsilon/N`。该概率随决策保存。默认没有探索的模型仍依赖先验；概率记录可用于后续评估，但当前实现不声称已完成离线反事实评估或消除了反馈选择偏差。

只有明确的 `Feedback(success=...)` 更新能力计数。其租户、模型及版本、策略版本必须匹配持久化决策；feature/category/difficulty 从决策读取，不能由反馈重写。一条决策最多训练一次；不同事件 ID 也不能追加另一条标签。缺失反馈不等于成功或失败。HTTP 层仍必须对质量反馈使用独立授权凭证，不能把网关状态码转成质量标签。

`choose_cluster()` 在已选模型的可用集群内比较同单位归一化的 TTFT、延迟、失败率、outstanding / soft_capacity，以及可选主动负载。缺失/过期时间指标使用集群先验，冷却中的集群排除。没有新样本且一段时间未分配请求的集群可以获得探测请求；已有 outstanding 的冷集群不会不断抢探测流量。探测仍只发生在已选模型的合格集群内，不绕过能力约束。

## 可选主动轮询

`TelemetryTarget.enabled` 默认 false。关闭时不需要 endpoint/凭证、不初始化 HTTP 客户端，也不访问租约表。开启后，`TelemetryPoller.run(stop=...)` 在后台周期轮询，路由请求不等待轮询。配置示例：

```yaml
cluster_id: model-a-east
enabled: false
endpoint: http://telemetry.example.internal/load
api_key_env: ADAPTIVE_TELEMETRY_KEY
load_path: gpu.utilization
divisor: 100
timeout_ms: 750
lease_seconds: 10
max_response_bytes: 65536
```

该配置用 GET 读取 `{"gpu":{"utilization":75}}`，归一化为 0.75。默认 `load_path: load`、`divisor: 1`，响应形状为 `{"load":0.75}`。只接受有限且归一化后在 0…1 的值。HTTP 有并发、总时间及字节上限，不跟随重定向、不解压、不继承环境代理。失败时保留旧值及原时间戳；旧值到期后失效，不伪造空闲负载零。

每个集群的 PostgreSQL 租约只允许一个副本轮询。持有者续租；副本退出后，其他副本可在租约到期后接管。写回时再次检查租约，旧持有者不能覆盖新持有者的数据。`lease_seconds` 需大于 HTTP timeout，并留出数据库和调度余量；后台退出时先发出 stop、等待当前轮询完成，再关闭 poller 和 store。

## 运行边界

数据库是唯一权威状态。连接失败、锁/语句超时会返回 `StateUnavailable`，不会切到进程内学习或返回伪造的空快照。每个事务独占连接，副本内有并发上限；总连接预算需考虑副本数，可使用支持事务模式的 PgBouncer。调用方还应设置完整 HTTP 请求 deadline。

生产环境需要 HA PostgreSQL 的可写 endpoint、持久卷、备份和 TLS/凭证；单个本机实例的测试不等于数据库 HA 部署。所有副本必须使用相同的目录版本、策略/特征版本和负载配置；改变模型定义时更新模型版本，改变集群含义时使用新的集群 ID。

当前不会自动删除决策或事件，因而重放/反馈去重在数据保留期间持续有效。运维应按业务允许的重放及反馈窗口安排保留策略，删除旧事件和决策前备份；删除后旧请求 ID 的幂等保证也随之结束。能力计数和集群状态应独立保留，不能用清空数据库代替发布升级。

事务与锁的实现参考 [PostgreSQL INSERT / ON CONFLICT](https://www.postgresql.org/docs/14/sql-insert.html)、[PostgreSQL 锁](https://www.postgresql.org/docs/14/explicit-locking.html) 和 [psycopg 异步并发](https://www.psycopg.org/psycopg3/docs/advanced/async.html)。
