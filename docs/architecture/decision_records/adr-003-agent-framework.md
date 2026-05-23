# ADR-003: 选择 LangGraph 替代 LangChain ReAct

## 状态
已接受

## 背景
原有 LangChain AgentExecutor 在复杂多步骤任务中无法并行执行节点，错误恢复能力弱。

## 决策
使用 LangGraph StateGraph + Supervisor 多 Agent 模式。

## 理由
- **并行节点**：LangGraph 支持 fan-out/join，多个诊断任务同时执行
- **状态管理**：TypedDict 状态显式管理，调试友好
- **条件路由**：Supervisor LLM 动态决策下一个 Agent，比硬编码更灵活
- **重试循环**：conditional edge 实现 verify → retry 循环，无需手动管理

## 代价
- 学习成本高于 AgentExecutor
- Graph 编译在首次调用时有约 1 秒开销
