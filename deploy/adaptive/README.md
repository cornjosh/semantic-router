# 部署分类服务与 Higress 插件

默认是一个分类进程、一个 SQLite 文件。只有需要跨节点多副本时才启用 etcd。最终大模型请求始终由 Higress 转发；这里不部署大模型推理引擎，也不需要 PostgreSQL。

## Docker 单实例

在仓库根目录设置 `GATEWAY_TOKEN`、`FEEDBACK_TOKEN`、`METRICS_TOKEN` 三个不同的随机凭证（至少 16 字符），然后：

```bash
docker compose -f deploy/adaptive/compose.yaml up --build -d
curl http://127.0.0.1:8080/readyz
```

这是分类服务的独立部署，持久数据位于 `classifier-data` volume。示例配置只有结构信号启用，不会访问付费模型。按 `examples/adaptive/service.yaml` 配置模型能力、集群、策略和外部信号 API；分类 API 说明见 [服务文档](../../docs/user-guide/features/adaptive-service.md)。Higress 插件的 classifier 地址应能访问此服务；Compose 默认只发布到本机环回地址，跨机器访问需按实际网络调整。

SQLite volume 必须位于支持正常文件锁的本地/块设备文件系统，不能使用 NFS。单实例重启可保留学习结果，但不提供跨节点 HA。旧 SQLite 使用 FULL 同步的回滚日志，新版本才启用修复后的 WAL；应用会检查不支持的网络挂载。

## 构建插件和服务镜像

```bash
docker build -f deploy/adaptive/Dockerfile -t YOUR_REGISTRY/adaptive-router:VERSION .
docker build -f deploy/adaptive/plugin.Dockerfile -t YOUR_REGISTRY/adaptive-plugin:VERSION plugins/higress-adaptive
```

插件镜像是 `scratch` 加 `/plugin.wasm`，采用 Higress 支持的自定义 OCI 格式。镜像发布由部署方按自己的仓库流程执行；本项目的脚本和 CI 不推送镜像。

没有 Docker daemon 时可以先生成、检查本地 OCI artifact：

```bash
uv run make -C plugins/higress-adaptive build OUT=/tmp/adaptive-routing.wasm
uv run python scripts/adaptive/package_wasm.py --wasm /tmp/adaptive-routing.wasm --output /tmp/adaptive-plugin-oci
skopeo inspect oci:/tmp/adaptive-plugin-oci:v0.1.0
```

该 artifact 的容器元数据标为 Linux amd64，与本地验证平台一致；payload 是 Wasm。它不包含 Python 服务。使用不同输出目录进行重复构建，脚本不会覆盖已有 artifact。

## Kubernetes 单实例

需要已安装 **Higress 2.2.5**、本项目构建的两个镜像，以及已存在的大模型后端 Service。服务 Chart 不会创建外部大模型，也不会修改已有模型的凭证。

先将 [higress-values.yaml](higress-values.yaml) 合并到 Higress 的 Helm 配置。该版本默认 `global.onlyPushRouteCluster=true`，会过滤没有公开路由引用的分类服务集群；本配置改为 `false`，使 Wasm 能通过内部集群名调用分类服务。它会增加推送的服务集群范围，已有大型网关应评估配置规模。使用官方固定版本 Chart 已渲染验证此开关确实生成 `PILOT_FILTER_GATEWAY_CLUSTER_CONFIG=false`。

```bash
helm repo add higress https://higress.io/helm-charts
helm upgrade higress higress/higress --namespace higress-system --version 2.2.5 \
  --reuse-values -f deploy/adaptive/higress-values.yaml
kubectl create namespace adaptive
```

将前述三个凭证保存为 `adaptive` 命名空间中的 `adaptive-credentials` Secret，键名分别为 `GATEWAY_TOKEN`、`FEEDBACK_TOKEN`、`METRICS_TOKEN`。另在 `higress-system` 创建 `adaptive-gateway-credentials` Secret，只包含同一个 `GATEWAY_TOKEN`，不要把质量反馈凭证交给网关。

例如，凭证已经在当前受控终端的环境变量中时：

```bash
kubectl -n adaptive create secret generic adaptive-credentials \
  --from-literal=GATEWAY_TOKEN="$GATEWAY_TOKEN" \
  --from-literal=FEEDBACK_TOKEN="$FEEDBACK_TOKEN" \
  --from-literal=METRICS_TOKEN="$METRICS_TOKEN"
kubectl -n higress-system create secret generic adaptive-gateway-credentials \
  --from-literal=GATEWAY_TOKEN="$GATEWAY_TOKEN"
kubectl -n higress-system patch deployment higress-gateway --type strategic \
  --patch-file deploy/adaptive/gateway-env.patch.yaml
```

固定版本的 Higress `gateway.env` Helm 参数只支持字符串值，所以 Secret 注入使用显式 Deployment patch。将 patch 纳入自己的 GitOps/post-renderer 流程，避免后续 Helm 升级丢失它。Secret 轮换后滚动重启网关，让 Wasm VM 读取新值。

复制 Chart 的 `values.yaml` 为部署参数文件，填写 `image`、`gateway.wasmUrl`、域名和 `gateway.upstreams` 中的实际 Service 名/端口；Service 与本 Chart 位于同一命名空间。所有候选 Ingress 必须共享相同的前置用户认证和租户授权。Chart 使用 `defaultConfig` 加 `hosts`，不使用会随重路由变化的 `matchRules`，也不同时运行另一个 `model-router`。

```bash
helm upgrade --install adaptive deploy/adaptive/chart --namespace adaptive \
  --set image=YOUR_REGISTRY/adaptive-router:VERSION \
  --set gateway.wasmUrl=oci://YOUR_REGISTRY/adaptive-plugin:VERSION \
  -f YOUR_DEPLOYMENT_VALUES.yaml
```

默认生成 1 个分类 Deployment、1 个 PVC、内部 Service、WasmPlugin、默认拒绝路由和按 `route_id` 匹配的候选 Ingress。SQLite 的 Deployment 使用 `Recreate`，Chart 拒绝把它扩成两个副本。数据 PVC 标记保留，不会因 Helm 卸载自动删除。修改存储模式前需要安排数据迁移；切换 backend 不会自动搬迁已有学习记录。

可通过 `--set-file serviceConfig=your-service.yaml` 提供完整分类配置；Chart 只按存储选项覆盖其 `state` 部分。对应地设置 `gateway.tenantId` 和全部 `gateway.upstreams` 映射。`gateway.namespace`、域名、IngressClass、TLS Secret 都可配置。TLS Secret 位于分类服务/Ingress 命名空间，设置 `gateway.tlsSecretName` 启用对外 HTTPS。

## 可选 etcd HA

已有业务 etcd 时设置 `storage.backend=etcd`、`replicas=2`、`storage.endpoints` 和 `storage.clientTLSSecret`；不部署额外 etcd。不要复用 Kubernetes 控制面内部的 etcd 数据库。客户端 Secret 包含 `ca.crt`、`tls.crt`、`tls.key`。

没有现成的业务 etcd 时，`values-ha.yaml` 可部署三成员集群。分类副本和 etcd 成员均要求不同宿主机；至少需要 3 个可调度节点及持久卷。准备证书后安装：

```bash
uv run python scripts/adaptive/create_etcd_tls.py \
  --destination /private/adaptive-etcd-tls --namespace adaptive --release adaptive
kubectl apply -f /private/adaptive-etcd-tls/secrets.yaml
helm upgrade --install adaptive deploy/adaptive/chart --namespace adaptive \
  -f deploy/adaptive/chart/values-ha.yaml -f YOUR_DEPLOYMENT_VALUES.yaml \
  --set image=YOUR_REGISTRY/adaptive-router:VERSION \
  --set gateway.wasmUrl=oci://YOUR_REGISTRY/adaptive-plugin:VERSION
```

证书脚本只创建本地文件，目录 0700、文件 0600，不联系集群。CA 私钥不进入 Secret；server/peer 与 classifier client 使用不同的密钥和证书。默认有效期 365 天，需要纳入部署方的证书续期流程；也可以使用自己的 PKI。Secret 名默认 `adaptive-etcd-server` 和 `adaptive-etcd-client`，SAN 根据 release 与 namespace 生成。

此配置增加 3 个 etcd Pod 和各自 2 GiB PVC，启用 client/peer 双向 TLS、PDB（至少 2 个 etcd 成员）、宿主机反亲和以及 etcd 入站 NetworkPolicy。分类服务至少保留 1 个可用副本；etcd 失去多数派时分类请求失败，不降级到分叉的本地学习状态。Higress 本身也应保持至少 2 个跨节点网关副本和既有的高可用控制面。

etcd 的 1 GiB backend quota 与 1 小时 revision compaction 不会自动删除当前决策/事件记录。需要按明确的业务幂等窗口安排归档/清理、备份和容量管理；不能把 etcd 当作无限增长的请求日志。保留窗口未确定时，本版本不会擅自删除幂等记录。更多存储语义见 [共享状态文档](../../docs/user-guide/features/adaptive-shared-state.md)。

## 可复现的验证与证据范围

```bash
uv run python scripts/adaptive/bootstrap_higress.py --destination /tmp/adaptive-tools --go --gateway
uv run python scripts/adaptive/bootstrap_etcd.py --destination /tmp/adaptive-tools
uv run python scripts/adaptive/bootstrap_kubernetes.py --destination /tmp/adaptive-kubernetes
export PATH="/tmp/adaptive-tools/go/bin:$PATH"
uv run make -C plugins/higress-adaptive test build OUT=/tmp/adaptive-routing.wasm
ETCD_BIN=/tmp/adaptive-tools/etcd HIGRESS_ENVOY=/tmp/adaptive-tools/envoy \
ADAPTIVE_WASM=/tmp/adaptive-routing.wasm HELM_BIN=/tmp/adaptive-kubernetes/helm \
KUBE_APISERVER=/tmp/adaptive-kubernetes/kube-apiserver \
  uv run pytest tests/unit/adaptive tests/integration/adaptive -q
```

下载脚本限 Linux amd64，使用固定 SHA256；`skopeo`、`openssl` 需要预先安装。Kubernetes 验证启动隔离的本机 API server/etcd，绑定环回地址，用临时客户端证书认证；不读取当前 kubeconfig，也不访问现有集群。

| 已执行的检查 | 能证明什么 |
| --- | --- |
| 标准 Go 构建 + 真实 Higress 2.2.5 Wasm | 上游选择、模型改写、SSE 字节保留、运行观测与故障处理 |
| 两台 Python 进程 + 三成员 etcd + 实际 Wasm/HTTP fixture | 共享决策、反馈去重与学习、单 worker 丢失、shortcut 跳过 provider、观测落库；fixture 不代表模型准确率 |
| 真 etcd 双向 TLS | 生成的 client/server 证书可读写 JSON API，缺少客户端证书被拒绝 |
| Helm 3.19.0 + Kubernetes 1.34.1 API + 固定 Higress CRD | 两种 profile 的严格服务端 dry-run 通过 |
| OCI layout + skopeo 检查/拷贝 | 本地插件镜像格式可被独立 OCI 工具读取 |

当前开发机器无 Docker daemon、无 Kubernetes 工作节点，因此没有声称在本机运行过完整 Pod 部署或服务容器。远程 [CI 39822517062](https://github.com/cornjosh/semantic-router/actions/runs/39822517062) 已成功构建两个 Dockerfile，并完成 SQLite 容器启动检查（提交 `a4496c0`）。本机真实 Kubernetes API 的严格 dry-run 与 CI 容器检查仍不等价于生产集群 Pod 调度、存储和网络验证。没有部署到生产集群，也没有发布镜像。

参考：[Higress 自定义插件](https://higress.io/docs/latest/plugins/custom/)、[固定版本网关环境变量模板](https://github.com/higress-group/higress/blob/4deae1dd3b4cb2707273748985e55bef28e747d1/helm/core/templates/_pod.tpl)、[固定版本集群过滤配置](https://github.com/higress-group/higress/blob/4deae1dd3b4cb2707273748985e55bef28e747d1/helm/core/templates/controller-deployment.yaml)。
