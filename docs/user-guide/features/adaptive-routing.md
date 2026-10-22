# 可配置的信号提取与策略决策

`semantic_router.adaptive` 将信息抽取与分发策略分开。信号只描述请求；策略根据事实给出候选模型，并决定是否停止后续信号处理。流水线返回的候选集合还需要经过服务层的能力、上下文、租户权限和集群可用性检查，不能直接当作已授权的最终转发目标。

## 运行无外部依赖的例子

从仓库根目录执行：

```bash
uv run python examples/adaptive/pipeline.py "解释一下二分查找"
uv run python examples/adaptive/pipeline.py --tool-result "查询结果是 42"
```

第一条返回 `general` 候选；第二条命中 `tool-continuation`，返回 `tool-model` 候选并跳过语义模型。两者都不调用外部 API。模型名是演示配置，不指向真实上游。

配置位于 `examples/adaptive/pipeline.yaml`，分为两部分：

```yaml
signals:
  - name: semantic
    kind: systemone
    enabled: false
    stage: 1
    timeout_ms: 500
policy:
  rules:
    - name: tool-continuation
      when:
        - signal: request
          path: is_tool_continuation
          op: eq
          value: true
      models: [tool-model]
      shortcut: true
  fallback_models: [general]
```

信号定义中没有目标集群或路由表；更改分发对象只改策略。`enabled: false` 的信号不会构造 extractor，也不会检查其凭证或发起网络请求。配置仍可保留引用该信号的规则；没有有效事实时这些规则不匹配。

## 执行顺序

1. 内置 `request` 信号提取便宜的结构信息：指定模型、工具需求、当前是否续接工具结果、输入模态、输出格式、生成上限等。
2. 策略按配置顺序检查规则。第一个匹配规则若设置 `shortcut: true`，流水线立即返回其候选，后续信号不初始化。
3. 否则按 `stage` 从小到大运行启用的信号。同阶段的信号并发执行，整个阶段完成后再检查策略。
4. 非 shortcut 匹配是暂定结果；所有阶段结束后，用完整信号重新按顺序匹配。没有规则匹配则返回 `fallback_models`；空 fallback 表示没有候选。

历史里出现过工具消息不等于当前正在续接工具调用。`has_tool_result` 表示历史存在工具结果，`is_tool_continuation` 只在最后一条消息为 `tool` 时成立。

规则中的多个 `when` 条件是 AND 关系。支持 `eq`、`in`、`gte`、`lte`、`exists`，点分路径访问嵌套对象。比较不把字符串 `"0.9"` 当作数字，不把 `true` 当作 `1`。缺失或 unavailable 信号不匹配，包括 `exists: false`；后者只能检查一个可用信号内部字段的缺失。

规则优先级是配置顺序：一个已匹配但没有 shortcut 的高优先级规则会阻止低优先级规则提前返回。将真正需要抢先处理的规则放在前面。

## 自定义信号

```python
from semantic_router.adaptive.config import PipelineConfig, SignalSpec
from semantic_router.adaptive.models import RoutingRequest
from semantic_router.adaptive.pipeline import SignalPipeline


class LanguageHint:
    async def extract(self, request: RoutingRequest) -> dict:
        # A real extractor can call a configured provider; it returns facts only.
        return {"language": "unknown"}


pipeline = SignalPipeline(
    config=PipelineConfig(signals=[SignalSpec(name="language", kind="language")]),
    registry={"language": lambda spec: LanguageHint()},
)
```

工厂只在信号首次真正执行时调用，并复用创建的实例。工厂应只做非阻塞初始化，网络工作放在 async `extract` 中。extractor 不接收策略或集群目录；返回值必须是有限数值组成的 JSON 数据。结果会复制并校验，不与 provider 持有的可变字典共享。

若 extractor 持有连接池，实现 async `aclose()`，并在服务退出时调用 `await pipeline.aclose()`。退出应先等待服务中进行的请求结束。关闭后的流水线不可再次使用。

## 超时与错误

`timeout_ms` 分别控制流水线总预算和单个信号预算。信号用完剩余预算时返回 `status: unavailable`、`error: timeout`；未开始且已无预算的信号不会初始化。调用方取消会传播给正在执行的信号，不转换为正常 fallback。自定义 extractor 必须遵守 asyncio 的协作式取消。

`required: true` 表示该信号一旦进入执行阶段，失败就抛出 `RequiredSignalUnavailable`，不能默默兜底。shortcut 明确跳过的后续 required 信号不会执行；不可通过关闭一个仍声明为 required 的信号绕过配置错误。

响应保留固定错误码，不包含供应商异常正文、API key 或用户 prompt。类别置信度、余弦相似度和模型任务成功率属于不同信号，不能混用。

`text_characters` 是完整消息历史中文本部分的字符数，包含 system 消息；它不是 token 数，也未把工具 schema 或图像换算成 token。目标模型的容量判断需要独立、明确的计数来源。

## 外部分类接口

`examples/adaptive/external-signals.yaml` 将 `providers` 与 `pipeline` 分开。前者配置完整 endpoint、服务端模型 ID、密钥环境变量和连接限制；后者配置要抽取的问题、文本来源、执行阶段和策略。示例中的模型名和 loopback 地址都需要替换，三个外部信号默认关闭：

```bash
# 无网络调用，验证默认结构信号和 fallback。
uv run python examples/adaptive/external_signals.py "解释二分查找"
# 配置实际 endpoint/model 并设置相应环境变量后，显式打开信号。
uv run python examples/adaptive/external_signals.py --config /path/to/signals.yaml --enable semantic
```

| kind | 协议与事实 | 分数含义 |
| --- | --- | --- |
| `systemone` | 一次请求发送 `state`、`model`、多个 choice 问题；返回各问题的标签和 `answers.<问题>.probabilities` / `confidence` | `provider_probability`，保留供应商分布，未在本项目数据上校准 |
| `embedding` | OpenAI-compatible embeddings；返回向量，或与 `examples` 的最大相似度及最近类别 | `embedding_vector` 或 `cosine_similarity`，相似度范围 −1…1，不是成功概率 |
| `llm` | OpenAI-compatible Chat Completions，要求支持 `response_format: json_schema`；每个问题返回枚举标签，加 `confidence` | `self_reported_confidence`，仅是分类模型自报估计 |

Jev 适配器复用本项目的 `choice()` 问题格式，支持自定义完整 endpoint，不自动拼接路径。首版将 category/difficulty 都表达为 choice 问题；没有伪装成兼容所有 System One 问题类型。响应必须包含全部问题、全部选项、有限的 0…1 数值，分布总和允许 0.01 的舍入误差；不会自动补零或重新归一化。choice 必须是最高概率选项，confidence 保留为独立统计量。协议依据 [TypeSafe System One 官方文档](https://docs.typesafe.ai/api/system-one) 和 [OpenAPI schema](https://api.typesafe.ai/openapi.json)，核对日期为 2026-10-22。

Embedding 按返回的 `index` 重排，检查维数、重复索引、非零范数和有限数字。配置 examples 后，首次实际执行才编码示例并缓存；之后只编码当前查询，使用本项目的 `similarity_matrix` 计算各类别最高余弦相似度。缓存是配置示例的派生数据，不存储用户查询。供应商返回的模型版本或维数改变时拒绝混算；更新 embedding 模型或示例后应重新创建流水线。对于相同 ID 下的隐式模型升级，客户端无法检测，应使用固定版本 ID。接口字段依据 [OpenAI embeddings reference](https://developers.openai.com/api/reference/resources/embeddings/methods/create)。

LLM 分类会验证实际返回内容，而非只信任供应商支持 schema。非法标签、额外字段、重复 JSON key、截断和 refusal 都返回 unavailable。它只输出分类事实，不生成用户的最终回复。所需 `json_schema` 协议见 [OpenAI Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=chat)；接入其他供应商前应确认其兼容范围。

每个信号的 `parameters.text.mode` 可选 `latest_user`（默认）或 `conversation`。前者只取最后一条 user 消息的文本；后者将完整角色序列、文本和工具调用作为待分类数据序列化。图像/音频数据不发送给这些文本分类器；消息中没有可用文本或超过 `max_chars` 时不可用，不静默截断。外部信号自己的 `text_characters` 表示实际送去分类的字符数，与 `request.text_characters` 的全历史结构统计不同，两者都不是容量计数器。

`ProviderRegistry.validate(signals=...)` 检查启用的配置和协议引用，不读取凭证或发请求。第一次真正调用时读取 `api_key_env`；配置了变量名却没有值时拒绝匿名调用。不需要认证的内部服务可省略该字段。多信号引用同一 provider 时共用连接池与并发额度。服务退出时先等待进行中的请求结束，再关闭 pipeline 和 registry：

```python
await pipeline.aclose()
await providers.aclose()
```

provider `timeout_ms` 包含排队、连接、响应头和完整响应体；还受 signal 与 pipeline 剩余预算约束。不自动重试或跟随重定向。请求和响应分别有字节上限，chunked 响应也受限制；只接受未压缩 JSON，避免压缩体突破内存边界。不继承环境代理配置。HTTP 使用默认 TLS 校验，endpoint 不能携带用户名、密码、query 或 fragment，密钥仅通过环境变量设置 Bearer header。

本机 HTTP 测试验证的是上述请求/响应合同与资源边界。测试回复均为人工构造数据，不代表真实供应商准确率或延迟。真实供应商尚需用实际 endpoint、固定模型版本和代表性数据验证。

另外提供 [公开数据集评测](../../../benchmarks/adaptive/README.md)：从 ModelScope 下载固定版本 BGE/Qwen 权重，通过实际 HTTP 适配器测试 MASSIVE、CLINC150 和 BANKING77。分类准确率、未知意图拒识、去重审计及局限见 [结果报告](../../../benchmarks/adaptive/REPORT.md)。Jev 未提供真实 endpoint/key，其准确率没有据此推测。
