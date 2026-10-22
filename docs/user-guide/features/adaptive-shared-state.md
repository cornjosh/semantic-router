# 持久状态、负载与在线学习

新路由服务默认用 **SQLite**，无需额外数据库服务。跨节点多副本使用可选的 **etcd**。两者实现同一个 `open_state()` 接口，使用相同的去重、负载统计和在线学习逻辑；不依赖 PostgreSQL，也不引入嵌入式 Raft。原有库的 `PostgresIndex` 不受影响。

| 部署模式 | 状态 | 额外进程 | 故障边界 |
|---|---|---|---|
| 单容器/单机 | SQLite 文件 | 无 | 进程重启后恢复；不承诺节点故障时持续服务 |
| 跨节点多副本 | 业务 etcd 集群 | 可复用已有业务集群；否则部署 3 个成员 | 3 成员可失去 1 个；失去多数派停止路由状态读写 |

不要连接 Kubernetes 控制面的 etcd。所有分类服务副本使用相同的业务 namespace/prefix。SQLite WAL 需要本地文件系统，不能用多个 Pod 共享 NFS 文件实现高可用；Linux 下会拒绝已识别的网络挂载。其他平台需自行保证文件系统满足 SQLite 锁和同步语义。

## 使用与本机复现

SQLite 使用标准库，无新增 Python 依赖。由部署提供数据目录，不会自动创建缺失的挂载目录：

```python
from semantic_router.adaptive.state.factory import open_state

store = open_state(backend="sqlite", path="/data/adaptive.db")
await store.initialize()
# 完成请求后/服务关闭时
await store.aclose()
```

etcd 复用已有 aiohttp 依赖，通过官方 v3 JSON gateway 访问。生产使用 HTTPS 和仅允许本 prefix 的应用身份；可配置 CA、客户端证书及 token 环境变量。token 轮换由部署提供，客户端每次请求重新读取环境值；不要把密钥写在 URL 或配置示例中。

```python
store = open_state(
    backend="etcd",
    endpoints=("https://etcd-0.example:2379", "https://etcd-1.example:2379", "https://etcd-2.example:2379"),
    prefix="/semantic-router/production/",
    token_env="ADAPTIVE_ETCD_TOKEN",
    ca_file="/run/secrets/etcd-ca.crt",
    timeout_ms=2000,
)
```

SQLite 测试不需要服务；etcd 测试会在本机启动并清理 3 个独立进程，数据位于 pytest 临时目录。必须使用本地临时盘：当前开发机器 `/mnt/workspace` 是 NFS，因此显式设置 `TMPDIR=/tmp`。

```bash
TMPDIR=/tmp uv run pytest tests/integration/adaptive/test_shared_state.py tests/integration/adaptive/test_storage_limits.py -q
# 取得固定版本和 SHA256 校验的 etcd 工具，再运行两个后端和真实故障测试
uv run python scripts/adaptive/bootstrap_etcd.py --destination /tmp/adaptive-etcd-tools
TMPDIR=/tmp ETCD_BIN=/tmp/adaptive-etcd-tools/etcd uv run pytest tests/integration/adaptive/test_shared_state.py tests/integration/adaptive/test_storage_limits.py tests/integration/adaptive/test_etcd_failover.py -q
```

可用 `ADAPTIVE_TEST_ETCD=http://127.0.0.1:2379,...` 指向专用测试集群；每个测试创建唯一 prefix。不要指向生产数据源。默认未指定 etcd 时只跳过 etcd 专属用例，SQLite 仍运行；CI 应提供 `ETCD_BIN` 验证两者。

## 决策、重放与观测

服务将完整 `Decision` 写入数据库后才能返回。`(tenant_id, request_id)` 原子比较写入和请求 fingerprint 保证并发重放返回同一条已提交决策；同 ID 不同内容返回冲突，不覆盖旧决策。决策记录不包含原始 prompt，保留模型版本、候选模型、实际选择概率、上下文桶和信号状态。决策 ID 也不能重用于另一请求。

一次决策对应一个逻辑转发 attempt。`Observation` 必须匹配决策中的租户、attempt 和集群。相同事件 ID、相同内容可重复提交；同事件 ID 改变内容会冲突。不同事件 ID 的重复 end/reset 回调以第一条终态为准，只更新一次。记录的 outstanding 是尚未结束且未到 `expires_at` 的决策数：重启不会清零，丢失终态会在有限时间后退出估计。它不保证精确的物理请求数，也不是容量预留。网关内部的物理重试应作为同一个逻辑 attempt 的耗时处理。

成功响应的延迟、可用的 TTFT、运行失败率分别更新 EWMA。timeout/upstream error 影响失败率与连续失败冷却；client cancel 只结束 outstanding，不被计为后端成功或能力失败。TTFT 未知保留 `None`，不补零。TTFT 与总延迟各自有时间戳；新非流式响应不会刷新旧 TTFT。过期的历史值在出现新样本时重新初始化，乱序/过期观测会结束对应 outstanding，但不会改写当前 EWMA。`samples` 表示实际更新 EWMA 的样本数，而非网关总请求数。

服务 UTC 时钟用于建议性的轮询租约、冷却和 outstanding 截止判断。租约写回同时比较所有读取版本，防止旧持有者覆盖新持有者。观测时间允许 5 秒时钟偏差，超过该范围的未来观测或明显早于决策的观测会拒绝。部署时需保证网关与所有服务节点时钟同步。

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

在时钟同步的前提下，每个集群的原子轮询租约用于减少副本间重复轮询。持有者续租；副本退出后，其他副本可在租约到期后接管。写回时再次检查租约，旧持有者不能覆盖新持有者的数据。`lease_seconds` 需大于 HTTP timeout，并留出数据库和调度余量；后台退出时先发出 stop、等待当前轮询完成，再关闭 poller 和 store。

## 运行边界

SQLite 每条连接使用 `synchronous=FULL`；SQLite ≥3.51.3 使用 WAL，旧系统运行库自动使用 rollback journal（DELETE），避免官方已披露的 [WAL-reset 并发损坏问题](https://www.sqlite.org/wal.html#the_wal_reset_bug)，无需增加依赖或部署步骤，短事务在受限线程池中运行，不阻塞 HTTP 事件循环。etcd 一次事务读取所需 key 的一致快照，按每个 key 的修改版本进行原子比较更新；冲突会在预算内退避重试，网络故障时轮换配置的 endpoint。已经提交但响应丢失的操作通过同一请求/事件 ID 重试，不重复训练或分配。

每次快照最多读取 64 个不同 key（模型数加集群数）；每条记录最多 512 KiB。每集群最多跟踪 4096 个未结束请求，可调低 `max_pending_per_cluster`；满额返回不可用，不创建未记账的决策。每次新增/结束决策清除过期 pending 项，快照忽略过期项。这里估计的是逻辑在途压力，不是 GPU 容量预留。应以压测确定适合业务的 QPS/并发，etcd 不作为海量请求日志仓库。

连接失败、锁超时、无多数派、容量限制都会返回 `StateUnavailable`；不会切到本地学习状态。SQLite 异步请求取消后，已经进入线程的事务仍可能完成；服务关闭会等待这些事务完成，重试保持原 ID 即可解决响应不确定性。HTTP 层仍需设置整个请求 deadline。

所有副本必须使用相同的模型目录版本、策略/特征版本和负载配置；改变模型定义时更新模型版本，改变集群含义时使用新集群 ID。上线需要配置本地持久卷或持久化的业务 etcd、备份与访问控制。SQLite 在线备份使用标准库 backup API，不要只复制正在使用的主文件而遗漏 WAL。

目前决策与事件不会自动删除，去重在记录保留期间有效。持续高流量部署前必须制定重放/反馈保留窗口并监控数据库容量；删除记录会结束对应 ID 的幂等保证。能力计数和集群状态独立保留，不可用清库代替发布升级。etcd 运维还需根据实际写入量配置历史压缩、快照和配额；服务不修改共享 etcd 的全局运维设置。

官方资料：[SQLite 适用场景](https://www.sqlite.org/whentouse.html)、[SQLite WAL](https://www.sqlite.org/wal.html)、[etcd v3 JSON gateway](https://etcd.io/docs/v3.7/dev-guide/api_grpc_gateway/)、[etcd API 一致性保证](https://etcd.io/docs/v3.7/learning/api_guarantees/)。
