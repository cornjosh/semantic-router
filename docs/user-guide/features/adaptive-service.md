# 分类决策 HTTP 服务

服务只接收分类所需的信息并返回逻辑模型、集群和路由 ID。Higress 负责转发原始生成请求与 SSE，Python 服务没有生成代理端点。默认一个进程、一个 SQLite 数据卷；多节点副本切换到业务 etcd，详见[持久状态](adaptive-shared-state.md)。实现复用已有 aiohttp，不增加服务器运行依赖。

## 启动

使用 [service.yaml](../../../examples/adaptive/service.yaml) 配置。默认语义信号关闭，只执行结构信号、规则、能力/容量检查以及负载/学习选择；启用外部信号前配置真实 endpoint。示例模型能力和 token 余量必须按实际后端修改。

```bash
export GATEWAY_TOKEN="$(openssl rand -hex 32)"
export FEEDBACK_TOKEN="$(openssl rand -hex 32)"
export METRICS_TOKEN="$(openssl rand -hex 32)"
docker compose -f deploy/adaptive/compose.yaml up --build -d
```

这是独立部署，不会启动 PostgreSQL 或 etcd。镜像以非 root 用户运行；命名卷持久化状态，服务端口默认只发布到 localhost。不要用 compose 的副本参数把 SQLite 变成跨节点部署。生产通过 Secret 注入独立密钥，不把上面的临时凭证输出或提交到仓库。

也可直接运行：准备**本地文件系统**上的数据目录，将 `state.path` 改为目录内文件，然后执行：

```bash
uv run python -m semantic_router.adaptive --config /path/to/service.yaml --host 127.0.0.1 --port 8080
```

每个进程使用一个事件循环。多副本配置相同策略/特征版本、模型目录和 etcd prefix，通过负载均衡接入。原有已记录决策的目标、模型版本或策略版本变化后，同 ID 重放会返回冲突，应换新 ID 发起新请求。

## 接口与凭证

| 接口 | 权限 | 作用 |
|---|---|---|
| `GET /healthz` | 无 | 进程存活 |
| `GET /readyz` | 无 | 持久状态可读且未进入退出阶段 |
| `GET /metrics` | metrics | 固定标签的 Prometheus 请求数、在途数和总耗时 |
| `POST /v1/route` | route | 保存并返回决策 |
| `POST /v1/observations` | observe | 幂等上报 TTFT/延迟/终态 |
| `POST /v1/feedback` | feedback | 幂等更新质量学习 |

使用 `Authorization: Bearer ...`；每个凭证绑定一个 tenant。网关凭证只能 route/observe，质量反馈必须使用独立凭证；配置中引用环境变量，不包含密钥值。启动时加载凭证，轮换后滚动重启服务。所有 POST 使用 `application/json` 和 `schema_version: "1"`。重复 JSON key、非有限数、超大/压缩输入均拒绝，不回显输入文本或内部异常。

```bash
curl --fail-with-body http://127.0.0.1:8080/v1/route \
  -H "Authorization: Bearer $GATEWAY_TOKEN" -H 'Content-Type: application/json' \
  -d '{"schema_version":"1","request":{"tenant_id":"demo","request_id":"demo-1","model":"auto","messages":[{"role":"user","content":"解释什么是语义路由"}],"max_tokens":128,"stream":true}}'
```

返回 `decision_id`、`attempt_id`、`request_id`、`model_id`、`model_version`、`cluster_id`、`route_id`、`upstream_model`、策略/特征版本、选择概率 `propensity`、原因、信号状态和容量记账依据。同 tenant/request_id 的相同请求重放返回相同内容，不重新调用语义 API；不同内容使用同 ID 返回 409。`route_id` 必须由网关映射到允许的上游，不能把响应当作任意目的 URL。

观测 envelope 为 `{"schema_version":"1","tenant_id":"demo","event":{...}}`；event 包含 `event_id`、`decision_id`、`attempt_id`、`cluster_id`、UTC `observed_at`、`outcome`、`latency_ms` 和可选 `ttft_ms`。`outcome` 取 `success/upstream_error/timeout/client_cancel`。TTFT 未知传 null，不填零。

质量反馈使用相同 envelope，event 包含 `event_id`、`decision_id`、实际 `model_id/model_version/policy_version` 和明确的布尔 `success`。只在有可信质量标签时调用，不能把 HTTP 200 当作模型回答正确。返回 `{"schema_version":"1","applied":true}`；重复有效事件返回 false。

## 约束与故障

固定模型、租户允许列表、工具/模态/输出格式、输出上限和上下文容量始终生效，包括 shortcut。结构信号提取完整 messages/tools/schema 的字节数和媒体计数；决策层用模型配置计算容量。`utf8_upper_bound` 是部署方声明的保守上界，不声称精确分词；媒体输入还需要显式预算。`exact_required` 在没有可信精确计数器时拒绝请求，不猜测。若未指定生成上限，预留该模型配置的最大输出量。

401/403 表示鉴权/权限失败；409 表示幂等或目标配置冲突；413 表示输入过大；415 表示不支持的编码/类型；422 表示请求无效或没有满足硬约束/策略的模型；503 表示过载、状态不可用、必要信号不可用或没有健康集群；504 表示整个请求超时。503/504 带 `Retry-After: 1`。重试保留原请求 ID 和内容，以处理已提交但响应丢失的情况。

服务有输入、并发、分类和存储超时上限。SIGTERM 停止接纳新请求并等待已有请求退出，再关闭轮询、连接池和状态后端。访问日志默认关闭；指标不以 prompt、请求 ID、任意用户输入为标签。被拒绝/失败的分类不会触发外部大模型生成。

## 验证范围

真实 HTTP 测试覆盖权限、租户、格式、冲突、输入限制、超时取消和过载；两个独立 Python 服务进程连接三个真实 etcd 成员，验证并发决策/反馈去重、进程被强制终止和重启后的共享学习。Docker/Compose 文件是部署定义，其实际镜像构建与 Higress 全链路证据另行记录，不能以 Python 测试替代。
