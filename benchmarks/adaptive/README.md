# 自适应分类与路由评测

本目录评测 **信号分类** 和 **路由执行** 两个不同问题。公开意图标签不能作为“最佳大模型”或“问题难度”的标签；合成质量反馈只验证学习机制。结果与局限见 [报告](REPORT.md)。

## 数据和权重

| 数据集 | 来源 | 场景及许可 |
|---|---|---|
| MASSIVE 1.0 zh-CN | [Amazon Science](https://github.com/alexa/massive) | 人工标注、翻译的助手语句，60 类；CC BY 4.0 |
| CLINC150 | [clinc/oos-eval](https://github.com/clinc/oos-eval) | 150 类意图及未知意图；CC BY 3.0 |
| BANKING77 | [PolyAI](https://github.com/PolyAI-LDN/task-specific-datasets/tree/master/banking_data) | 银行客服，77 类；CC BY 4.0 |

版本、下载地址、逐文件 SHA256 和许可证文件的摘要均在 [provenance](provenance/)。引用或再分发数据时应保留原作者、论文及许可证；仓库不包含原始数据和权重。BGE 使用 MIT，Qwen3 使用 Apache 2.0，以固定版本的模型卡为准。

模型来自 ModelScope：

- [bge-small-zh-v1.5](https://modelscope.cn/models/AI-ModelScope/bge-small-zh-v1.5)，24M，512 维。
- [bge-small-en-v1.5](https://modelscope.cn/models/AI-ModelScope/bge-small-en-v1.5)，33M，384 维。
- [Qwen3-0.6B](https://modelscope.cn/models/Qwen/Qwen3-0.6B)，用于小规模结构化分类探测。

下载和校验（Python 3.12；`--models` 需要当前 Python 环境安装 ModelScope）：

```bash
uv run python -m benchmarks.adaptive.download --data-root /tmp/adaptive-data
uv run python -m benchmarks.adaptive.download --data-root /tmp/adaptive-data \
  --models --model-cache /tmp/adaptive-models
```

下载器只取固定 revision 的 safetensors 权重，模型加载使用 `local_files_only=True`、`trust_remote_code=False`，不会运行远程模型代码。`models.json` 记录实际文件哈希，重新下载后逐一验证。工具环境不加入分类服务运行时依赖。

## 重跑分类评测

先在装有 PyTorch、Transformers 和 aiohttp 的单独环境启动本地模型服务。`--model-path` 使用下载器打印的 snapshot 目录。该服务仅监听 127.0.0.1，用于评测；它不是生产大模型服务。

```bash
uv run python -m benchmarks.adaptive.local_provider \
  --model-path /PATH/TO/ZH_SNAPSHOT --model-id bge-small-zh-v1.5 --port 18089
```

另一终端从本项目环境运行：

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 uv run python -m benchmarks.adaptive.evaluate \
  --data-root /tmp/adaptive-data --dataset massive-zh \
  --endpoint http://127.0.0.1:18089/v1/embeddings --model-id bge-small-zh-v1.5 \
  --output /tmp/massive-zh-bge.json
```

将模型服务切换为英文 BGE 后，分别设置 `--dataset clinc150` 和 `--dataset banking77`、`--model-id bge-small-en-v1.5`。完整测试集会逐请求通过项目 `SignalPipeline → ProviderRegistry → HTTP → GPU`；不是预计算向量后冒充 HTTP 延迟。

每类从训练集取最多 5 条示例，seed=42，不微调。BGE 使用 CLS、L2 归一化，无检索前缀，输入超过 512 tokens 时明确报错，不静默截断。原型一次初始化并缓存，后续查询每次调用模型。原型初始化可能超过默认响应上限，评测显式使用 16 MiB 响应限额和 60 秒超时；这是评测配置，不是建议的线上延迟预算。

先按 NFKC、casefold、折叠空白规范化去重，train 优先于 dev，dev 优先于 test；同时去掉 split 内重复。MASSIVE 保留官方 split，CLINC 保留官方 split 并合并 dev/test 的 OOS；OOS 训练数据不用作示例。BANKING77 对训练语句先去重，再按类保留 20% 为 dev。原始数量、剔除数量和原型文本哈希随结果保存。

CLINC 的拒识阈值只在 dev 上选择，最大化 ID 接受率与 OOS 召回率的平均值；平分时选较低阈值。测试集报告原始 ID 准确率、macro-F1、覆盖率、接受部分准确率、OOS 召回和误接受率。余弦分数与 LLM 自报置信度都不是校准概率。无效输出计为错误，不从分母删除。

Qwen 小样本探测：模型服务加 `--protocol llm`，使用对应 snapshot 和 `--model-id Qwen3-0.6B`；评测参数为：

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 uv run python -m benchmarks.adaptive.evaluate \
  --data-root /tmp/adaptive-data --dataset massive-zh --protocol llm --per-class 1 \
  --endpoint http://127.0.0.1:18091/v1/chat/completions --model-id Qwen3-0.6B \
  --output /tmp/massive-zh-qwen-probe.json
```

从 dev、test 各类固定采样 1 条，seed=43；零样本、greedy、关闭 thinking、最多 128 输出 token。模型服务不提供 JSON constrained decoding，只遵从适配器提示，原样返回模型输出；非法 JSON、标签或字段均由生产适配器拒绝并记入错误。这个小样本结果不能代替完整数据集评测，也不与 BGE 全量结果直接排名。

## 路由机制与延迟

```bash
uv run python -m benchmarks.adaptive.workload --samples 200 --warmup 20 \
  --output /tmp/workload.json
```

通过真实本机 HTTP 和 `/tmp` 上的 SQLite 分别运行静态、负载感知、质量反馈学习、外部信号、工具 shortcut 五种场景。每种预热 20 次、测量 200 次，并发 1；回报 HTTP 只发生在路由计时之后。外部 embedding fixture 固定等待 20ms；后端 TTFT、延迟和成功标签是脚本给定，报告明确使用 `synthetic` 标记。学习和负载场景保留预热决策轨迹，以便核对变化原因。这里的 p50/p95/p99 是分类服务响应时间，不是完整生成 TTFT 或系统吞吐上限。

完整代理及故障切换证据在 [部署说明](../../deploy/adaptive/README.md)；不需要为数据集评测部署 etcd 或 Kubernetes。

代理开销另外通过真实 Higress v2.2.5 和编译后的插件测量：

```bash
HIGRESS_ENVOY=/PATH/TO/envoy ADAPTIVE_WASM=/PATH/TO/adaptive.wasm \
  uv run python -m benchmarks.adaptive.gateway --samples 200 --output /tmp/gateway.json
```

该脚本复用开发环境的真实代理测试启动器，测量直接访问 SSE fixture 和经过 Higress/Wasm/SQLite 分类服务两条路径，分别使用并发 1、8。fixture 固定 20ms 后输出内容、再过 2ms 输出 DONE；客户端测量 TTFT、完整响应延迟及请求/秒，并逐条核对 SSE 字节。两路径分位数之差是环境内的开销估计，不是跨机器 SLA。报告包含请求数、预热数、错误数、Wasm/Envoy SHA256。
