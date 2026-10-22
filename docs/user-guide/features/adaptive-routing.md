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
