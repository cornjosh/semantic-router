# 自适应 Higress 路由交付核对

实现分支：`feat/higress-adaptive-routing`；基础提交 `1fa1937e390fe57fb85ee3bbb8b3db593a4145db`。最初[现场调研](2026-10-22-higress-pd-discovery.md)中的 PD 和 PostgreSQL 设想已被后续用户确认及[实现规格](../superpowers/specs/2026-10-22-adaptive-higress-routing.md)替代。

## 需求与证据

| 用户要求 | 最终行为 | 证据入口 |
|---|---|---|
| Higress 插件调用分类服务，生成由网关转发 | Wasm 提取必要字段；服务返回允许的目标元组；网关改写模型及内部路由头并继续原请求 | `plugins/higress-adaptive/`；`test_higress_wasm.py`；`test_complete_chain.py` |
| 不实现外部生成模型 | Python 只有 route/observations/feedback/health/metrics；测试上游为明确标识的 fixture | `semantic_router/adaptive/app.py`；真实代理字节一致性测试 |
| 首版不含 PD | 先选择符合约束的模型，再从该模型的多个集群选择；没有推理调度器或资源预约 | `selection.py`、`constraints.py`、`service.py` |
| TTFT、延迟等判断负载 | 被动 EWMA、失败率、冷却、数据新鲜度、在途估计；可配置主动遥测 | `state/store.py`、`telemetry.py`；共享状态和负载选择测试 |
| 多副本高可用 | 两个独立服务进程共享业务 etcd；线性一致读、CAS 写、失败不降级本地 | 三成员 etcd 的 leader/quorum/磁盘重启测试；双进程及完整链路故障切换 |
| 在线学习 | 可信质量反馈更新上下文 Beta 计数；epsilon 可配置；概率留存；反馈幂等 | `state/store.py`、`choose_model`；跨进程重复反馈及模型切换测试 |
| 信号与决策分开 | signal 只返回事实；声明式规则选择候选及 shortcut；硬约束再过滤 | pipeline/config/policy 测试；工具 shortcut 跳过真实 HTTP fixture |
| 可关闭不必要信号 | disabled 不初始化、不读密钥、不联网；shortcut 跳过后续阶段 | pipeline/provider 测试及合成调用次数统计 |
| 不增加 PG、尽量简单 | 默认独立进程 + SQLite + 本地卷；可复用业务 etcd，或可选三成员 HA profile | [存储说明](../user-guide/features/adaptive-shared-state.md)、[部署说明](../../deploy/adaptive/README.md) |
| Docker/Kubernetes、最新 Higress release | 固定核对后的 Higress 2.2.5；Dockerfile、Compose、Helm、Secret、TLS 和 OCI 插件打包 | 真实官方 Envoy/Wasm；实际 K8s API 严格 dry-run；远程 Docker 构建及容器 smoke |
| ModelScope 轻量模型、真实分类准确率 | 固定 BGE 中英文及 Qwen3-0.6B；MASSIVE、CLINC150、BANKING77；实际 HTTP 适配器 | [评测方法](../../benchmarks/adaptive/README.md)、[结果](../../benchmarks/adaptive/REPORT.md)、逐文件摘要 |
| 每个完成功能推远程 | 分功能提交至用户指定 feature branch，保留原主分支 | Git 提交历史及远程 SHA 核对 |

## 验证边界

本机 Ubuntu 22.04 容器，8 CPU、约 28 GiB 内存、NVIDIA A10；工作区为 NFS，SQLite 与临时运行目录使用 `/tmp` 的本地文件系统。固定的工具下载及校验脚本在 `scripts/adaptive/`；模型、数据下载器在 `benchmarks/adaptive/`。完整版本清单见 `benchmarks/adaptive/provenance/environment.json`。

本机没有 Docker daemon 或 Kubernetes worker，因此本机真实代理/etcd/分类服务测试采用进程启动；Kubernetes 验证为隔离的真实 API server 和 etcd，不使用现有 kubeconfig。[远程 CI](https://github.com/cornjosh/semantic-router/actions/runs/39822517062) 已实际构建分类服务镜像与插件镜像，并启动 SQLite 容器完成 readiness 检查。生产 Pod 调度、PVC 存储类型、CNI、域名、证书及外部模型凭证仍需部署方配置；没有执行生产部署、合并主分支或发布镜像。

Jev 采用官方 System One choice 协议，实际 HTTP fixture 已验证，但没有真实 Jev endpoint/key，因此不报告 Jev 模型准确率。外部生成模型质量不在此次实现范围；公开意图数据和合成反馈都不能证明自动选择了回答质量最高的模型。

SQLite 单实例不提供节点故障容错；跨节点 HA 使用 etcd。异步观测队列是有界的 best effort，网关崩溃可能丢观测；决策与质量反馈由持久状态保证，未完成在途记录按 TTL 失效。一次决策代表逻辑尝试，不能当作物理重试的并发预约。没有自动删除幂等记录，部署方需要明确保留窗口及容量计划。

## 验证记录

本机非付费 unit、functional 与全部 adaptive integration 回归：**595 passed、29 skipped**，78.67 秒；3 条警告来自既有 Qdrant 客户端测试。全库 ruff check/format 通过（157 个文件），mypy 79 个源文件通过，Go race/vet 与 Wasm 构建通过。原库需要 pinecone-local/pgvector/qdrant 容器的集成套件因本机无 Docker daemon 未运行，付费 live 测试未运行。新服务的 SQLite/etcd、HTTP、真实 Higress 和 Kubernetes API 测试实际运行，不归入该环境缺口。

测试日志留在本机 `/tmp/adaptive-*`；版本化评测结果、运行环境及输入摘要随分支保存。独立审查与最终远程 SHA 在结束前补充。

## 实施取舍记录

以下按执行顺序保留裁决及其代价；历史 PG 连接方案已经撤销，不属于最终实现。

1. 按用户的一次边界确认自主执行；常规选择不再请求许可。代价是方案偏好需要由交付审查核对。
2. 在线学习选择上下文 Beta + 可配置 epsilon-greedy，延迟另用 EWMA。代价是它不建模连续特征，依赖真实反馈覆盖及合理探索设置。
3. 网络慢时从已核验的公开 Aliyun PyPI 镜像安装锁定哈希的依赖，锁文件不变。代价是复现需能访问至少一个依赖镜像。
4. 新配置测试重命名以避免既有 mypy 模块名冲突，示例使用类型匹配的 tuple。代价仅是文件名差异。
5. 重用现有 choice 格式和 similarity kernel，新增有界 HTTP 传输。代价是适配器需要单独维护协议边界，换取禁重试、截止时间与体积限制。
6. Jev 首版只采用 choice 表达类别/难度，不声称支持其所有问题类型。代价是新增类型需要扩展和验证。
7. 每个决策一个逻辑尝试，首个终态生效；在途从未过期决策推算。代价是不反映物理重试的精确 GPU 占用。
8. 历史 PostgreSQL 单事务连接/PgBouncer 方案随用户禁止 PG 已撤销；保留窗口需显式定义的原则仍有效。最终只有 SQLite/etcd。
9. 本机用固定官方 OCI 中提取的真实 Higress binary 验证 Wasm。代价是这种证据本身不涵盖容器及 Pod；容器证据后来由 CI 补充。
10. 插件负责改写模型，目标为 OpenAI-compatible 上游；同路径禁用另一个 model-router。代价是厂商专用 ai-proxy 转换需另行验证。
11. 根上下文持有有界观测队列，避免请求销毁导致回调丢失。代价是队列满或网关崩溃时观测可能丢失，需监控丢弃指标。
12. SQLite profile 为单副本/Recreate；HA profile 为双分类服务和可选三成员 etcd。代价是捆绑 HA 需要三节点、持久卷、TLS 轮换及容量管理。
13. Higress `onlyPushRouteCluster=false` 使内部分类服务可被 Wasm 访问。代价是网关获得的集群配置范围扩大，大规模安装需评估配置体积。
14. 通过网关 Deployment Secret env patch 与 VM HOST 环境传密钥。代价是 GitOps 需要保留补丁，轮换后重启网关。
15. 数据集标准化去重，train/dev/test 依次优先。代价是数量与官方未过滤榜单不同，不能直接排名比较。
16. BANKING77 带问号的类名只在传输层转换成稳定别名，统计时还原。代价是复现必须保存别名映射；没有删掉该类或放松生产标识符校验。
