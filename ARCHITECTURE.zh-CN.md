# CrewMeld 核心流程架构

> 本文档描述 CrewMeld 平台的工具、API、SOP 编排层的核心调用链与关键文件位置。
> 适用于开发者快速理解编排层架构。最后更新：2026-11-23。

---

## 目录

- [架构总览](#架构总览)
- [1. 对话引擎（Conversation Runtime）](#1-对话引擎conversation-runtime)
- [2. SOP 编排引擎](#2-sop-编排引擎)
- [3. 工具执行核心](#3-工具执行核心)
- [4. 系统连接与工具注入](#4-系统连接与工具注入)
- [5. 完整调用链](#5-完整调用链)
- [6. 关键设计要点](#6-关键设计要点)

---

## 架构总览

```
用户消息 / Cron / 手动触发
  ↓
┌───────────────────────────────────────────────────────┐
│  对话引擎 conversation/engine.ts                       │
│  意图分类 → 知识库检索 → LLM 决定调用工具              │
└───────────────────────────────────────────────────────┘
  ↓ sop_*              ↓ skill_*            ↓ 内置工具
┌──────────────┐  ┌─────────────────────┐  ┌──────────┐
│ SOP 引擎     │  │ 工具执行器           │  │ check_   │
│ sop/engine   │  │ llm-tool-executor   │  │ sop_*    │
│ 状态机+节点  │  │ api/http/script     │  │ query_   │
└──────────────┘  └─────────────────────┘  │ my_tasks │
  ↓                                        └──────────┘
┌───────────────────────────────────────────────────────┐
│  节点分派器 sop/node-executor.ts                       │
│  digital_employee / human_employee / human_confirm /  │
│  switch                                               │
└───────────────────────────────────────────────────────┘
  ↓
┌───────────────────────────────────────────────────────┐
│  外部系统层（通过系统连接）                             │
│  数据库 / 自定义 API / CRM / ERP / RAGFlow / n8n ...   │
└───────────────────────────────────────────────────────┘
```

平台围绕三类资产组织 AI 能力：**数字员工**（任务执行主体）、**SOP**（多角色协作流程）、**工具**（原子能力）。编排层是粘合这三者的核心。

---

## 1. 对话引擎（Conversation Runtime）

### 核心文件

| 文件 | 关键函数 | 职责 |
|------|---------|------|
| `lib/conversation/engine.ts` | `processMessage()` (L120) | 对话入口：加载历史 → 意图分类 → 知识库检索 → 进入消息循环 |
| 同上 | `runMessageLoop()` (L902) | 多轮 tool_call 循环（最多 5 轮）：调 LLM → 解析 tool_calls → 逐个执行 → 结果回注 → 继续 |
| 同上 | `callLLM()` (L1597) | 调 OpenAI 兼容流式 API，带 `tools` + `tool_choice: 'auto'` |
| 同上 | `parseSSEStream()` (L1635) | 解析流式响应，流式分片拼接 tool_calls |
| `lib/conversation/intent-router.ts` | `buildWorkflowToolConfigs()` (L46) | 将员工绑定的 SOP + 技能转为 LLM tool 定义 |
| `lib/conversation/sop-bridge.ts` | `executeSopFromConversation()` (L88) | 对话中触发 SOP 执行 |

### 执行流程

```
processMessage()
  │
  ├─ 1. 加载对话 + 数字员工 (L142-180)
  ├─ 2. 保存用户消息 + OCR 上下文 + 文件附件 (L188-244)
  ├─ 3. 加载消息历史 + 过期工具结果清理（5 分钟）(L270-367)
  ├─ 4. 语言检测 (L371-517)
  ├─ 5. prepareConversationRuntime() — 并行加载身份、模型配置、知识库、工具 (L611-640)
  ├─ 6. classifyIntent() — 决定 knowledge_only / knowledge_then_sop / sop_first (L645)
  ├─ 7. 知识库检索 (L683-728)
  ├─ 8. 构建系统提示词 (L743)
  └─ 9. runMessageLoop() (L785)
        │
        └─ 循环（≤ MAX_TOOL_ROUNDS = 5）：
             ├─ callLLM() → POST /chat/completions (stream, tools)
             ├─ 无 tool_calls → 保存 → SSE 推送 → 结束
             └─ 有 tool_calls → 逐个分派：
                  ├─ sop_*       → executeSopFromConversation()  → 见第 2 节
                  ├─ skill_*     → HTTP POST 已部署端点 / OpenClaw 异步派发
                  ├─ check_sop_status → 查询 SOP 执行状态
                  └─ query_my_tasks   → 查询用户任务列表
             → 工具结果注入 messages → 继续循环
```

### 工具定义构建

`intent-router.ts` 中的 `buildWorkflowToolConfigs()` 负责将员工关联的能力转为 LLM 可识别的 tool 定义：

1. 查询员工关联的活跃 SOP（扫描节点中 `executorId === employeeId`）
2. 按可见性三态分桶（allow / deny / hide）
3. 为每个可见 SOP 生成 `sop_<id>` 工具
4. 注册 `check_sop_status` 和 `query_my_tasks` 内置工具
5. 无可见 SOP 时退化为暴露员工绑定的技能工具

---

## 2. SOP 编排引擎

### 核心文件

| 文件 | 关键函数 | 职责 |
|------|---------|------|
| `lib/sop/engine.ts` | `executeSop()` (L388) | 主状态机循环：执行节点 → 评估出口 → 下一节点 |
| 同上 | `VALID_TRANSITIONS` (L49) | 8 种状态的合法转换定义 |
| 同上 | `transitionStatus()` (L68) | 原子状态转换（乐观锁 + SSE 事件 + 统计更新） |
| 同上 | `resumeSopFromPause()` (L722) | 人工审批后恢复执行 |
| 同上 | `resumeSopFromAsyncTool()` (L912) | 异步工具回调后恢复执行 |
| 同上 | `recoverSopInstances()` (L1187) | 进程启动时冷恢复未完成的 SOP |
| `lib/sop/node-executor.ts` | `executeNode()` (L273) | 节点类型分派器 |
| 同上 | `executeDigitalEmployeeWithLLMTools()` (L885) | 数字员工节点执行 |
| 同上 | `executeGatewayNode()` (L301) | 条件分支网关（LLM 评估或规则比较） |
| `lib/sop/scheduler.ts` | `processScheduledTask()` (L127) | BullMQ 定时触发 |
| `lib/sop/llm-tool-executor.ts` | `executeLLMWithTools()` (L158) | SOP 内的 LLM 多轮工具调用循环 |

### 状态机

```
                    ┌──────────┐
                    │ pending  │
                    └────┬─────┘
                         ↓
                    ┌──────────┐
              ┌─────│ running  │─────┐
              │     └────┬─────┘     │
              ↓          ↓           ↓
     ┌────────────┐ ┌──────────┐ ┌──────────┐
     │paused_human│ │paused_   │ │completed │ (终态)
     └─────┬──────┘ │tool      │ └──────────┘
           │        └────┬─────┘
           ↓             ↓
     ┌──────────┐  ┌──────────┐
     │ running  │  │ running  │  (恢复后继续循环)
     └──────────┘  └──────────┘

     其他终态：timed_out / failed / cancelled
```

### 节点类型

| 节点类型 | 执行函数 | 说明 |
|---------|---------|------|
| `digital_employee` | `executeDigitalEmployeeWithLLMTools()` (L885) | 数字员工执行：构建工具 → 组装提示词 → LLM 工具循环 |
| `human_employee` | `executeHumanEmployeeNode()` (L1677) | 暂停等待人工提交 → `paused_for_human` |
| `human_confirm` | `executeHumanConfirmNode()` (L1757) | 读取上游审批决策，路由到对应分支 |
| `switch` | `executeGatewayNode()` (L301) | 条件分支：LLM 评估或字段提取 + 规则比较 |

### SOP 触发方式

| 触发源 | 入口文件 | 函数 |
|--------|---------|------|
| 对话触发 | `lib/conversation/sop-bridge.ts` | `executeSopFromConversation()` (L88) |
| 定时触发 | `lib/sop/scheduler.ts` | `processScheduledTask()` (L127) — BullMQ repeatable job |
| 手动触发 | `app/api/employee/sops/[id]/execute/route.ts` | `_POST()` (L17) |

---

## 3. 工具执行核心

### 核心文件

| 文件 | 关键函数 | 职责 |
|------|---------|------|
| `lib/sop/llm-tool-executor.ts` | `executeLLMWithTools()` (L158) | SOP 内 LLM 多轮工具调用循环（同步模式） |
| 同上 | `runAsyncToolLoop()` (L853) | 异步模式：工具派发后挂起 SOP，回调恢复 |
| 同上 | `resolveToolCall()` (L134) | 工具分类器：`builtin > apitool > http-endpoint > unknown` |
| `lib/tools/api-tool-runner.ts` | `runApiTool()` (L63) | 进程内 API 工具执行：pre → request → post 三阶段 |
| `lib/tools/script-invoker.ts` | `invokeScriptTool()` | 脚本工具执行（OpenSandbox 容器） |
| `app/api/employee/tools/execute/route.ts` | `_POST()` (L78) | 工具代码测试入口（沙箱/子进程执行） |
| `app/api/employee/skills/instances/[id]/invoke/route.ts` | `POST()` (L16) | 已部署技能实例调用 |
| `app/api/sop/[execId]/tool-callback/route.ts` | `POST()` (L27) | 异步工具回调入口（HMAC 鉴权） |

### 三条工具执行路径

```
LLM 决定调用工具
  ↓ resolveToolCall() 分类
  ├─ apitool  → runApiTool()          进程内执行（JS 沙箱）
  │             pre → request → post
  ├─ http     → fetch() POST           已部署的 K8s/OpenSandbox 端点
  └─ script   → invokeScriptTool()     OpenSandbox 容器内执行
```

### API 工具三阶段流水线

`runApiTool()` (api-tool-runner.ts L63) 的执行流程：

1. **pre 阶段**：沙箱 JS 代码生成请求参数（headers、body、query）
2. **request 阶段**：调用 `callCustomApi()` 发送 HTTP 请求
3. **post 阶段**：沙箱 JS 代码转换响应格式

支持 `forwardIdentity`：GET/HEAD/DELETE 注入 `X-Identity` header，POST/PUT/PATCH 注入 body `.identity` 字段。

### 异步工具回调

```
工具 Pod 执行完成
  ↓ POST /api/sop/[execId]/tool-callback (callId + HMAC token)
  ↓
handleToolCallback()
  ↓ finalizeToolResult() → completeToolCallLog() (First-Wins)
  ↓ afterToolCallTerminal() — 销毁 Pod
  ↓ 该 task 所有 pending 调用归零
  ↓ resumeSopFromAsyncTool() — 重建 LLM 对话 → 重新执行挂起节点
```

---

## 4. 系统连接与工具注入

### 核心文件

| 文件 | 关键函数 | 职责 |
|------|---------|------|
| `lib/connectors/resolve-conn-env.ts` | `resolveConnectionEnvVars()` (L94) | 按 connectionId 查 DB → 解密配置 → 转为 `CONN_*` 环境变量 |
| 同上 | `configToConnEnvVars()` (L27) | 配置对象 → 环境变量字典（如 `apiKey` → `CONN_API_KEY`） |
| 同上 | `configKeyToEnvName()` (L13) | camelCase → `CONN_UPPER_SNAKE` 转换 |
| 同上 | `resolveConnectionEnvKeys()` (L67) | 仅返回 key 名称（不含值），供 AI agent 使用 |

### 支持的连接类型

```
wecom, dingtalk, feishu, discord, crm, database, custom_api,
openclaw, dify, n8n, email, telegram, ragflow, baidu_ocr, wxoa
```

其中 `database` 子类型：postgresql、mysql、mariadb、sqlserver、oracle、mongodb、redis。

### 注入机制

系统连接的加密配置在工具执行时被解密并注入为 `CONN_*` 环境变量：

```
system_connections.configEncrypted (加密)
  ↓ 解密
config = { host, port, database, username, password, ... }
  ↓ configToConnEnvVars()
CONN_HOST=db
CONN_PORT=5432
CONN_DATABASE=crewmeld
CONN_USERNAME=postgres
CONN_PASSWORD=*****
CONN_TYPE=database
```

调用点：`tools/execute/route.ts`(L128)、`skills/instances/[id]/deploy`、`skills/instances/[id]/test-run`、`dev-studio/sandbox-loader.ts`。

---

## 5. 完整调用链

### 链路 A：用户对话 → 工具调用

```
用户消息
  ↓
conversation/engine.ts: processMessage() (L120)
  ↓ 加载对话/员工/历史 → 意图分类 → 知识库检索
  ↓
conversation/engine.ts: runMessageLoop() (L902)
  ↓
callLLM() (L1597) → POST /chat/completions (stream, tools, tool_choice=auto)
  ↓
  ├─ LLM 返回纯文本 → 保存 → SSE 推送 → 结束
  └─ LLM 返回 tool_calls → 逐个分派：
      ├─ sop_*   → sop-bridge.ts: executeSopFromConversation() → 见链路 B
      ├─ skill_* → HTTP POST 已部署端点 / OpenClaw 异步派发
      ├─ check_sop_status → 查询 SOP 执行状态
      └─ query_my_tasks   → 查询用户任务列表
  ↓ 工具结果注入 messages
  ↓ 继续循环（最多 5 轮）
```

### 链路 B：SOP 执行 → 节点分派 → 工具调用

```
SOP 触发（三种来源）：
  ├─ 对话触发: sop-bridge.ts: executeSopFromConversation() (L88)
  ├─ 定时触发: sop/scheduler.ts: processScheduledTask() (L127)
  └─ 手动触发: api/employee/sops/[id]/execute/route.ts (L17)
  ↓
写 sopExecutions (pending) → transitionStatus(pending → running)
  ↓
sop/engine.ts: executeSop() (L388) — 主状态机循环
  ↓ 从 stateSnapshot.currentNodeId 开始
  ↓
sop/node-executor.ts: executeNode() (L273) — 按节点类型分派
  ├─ digital_employee → executeDigitalEmployeeWithLLMTools() (L885)
  │   ↓ 构建工具定义 → 组装系统提示词
  │   ↓
  │   sop/llm-tool-executor.ts: executeLLMWithTools() (L158)
  │     ↓ LLM 决定调用工具 → resolveToolCall() (L134)
  │     ├─ apitool → runApiTool() — 进程内执行
  │     ├─ http    → fetch() POST 到 K8s/OpenSandbox 端点
  │     └─ script  → invokeScriptTool() — 容器执行
  │     ↓ 工具结果注入 messages → 继续循环
  │     ↓ LLM 输出 summary → 返回 NodeExecutionResult
  │
  ├─ human_employee → 暂停 → 发送通知 → paused_for_human
  │   ↓ 人工决策 → resumeSopFromPause() (L722) → 继续
  │
  ├─ human_confirm → 读取审批结果 → 路由分支
  │
  └─ switch → LLM 评估分支 / 字段提取 + 规则比较
  ↓
evaluateExits() → 下一节点 → 继续循环
  ↓ 无下一节点 → completed → 通知对话/IM 渠道
```

### 链路 C：异步工具回调恢复

```
工具 Pod 执行完成
  ↓ POST /api/sop/[execId]/tool-callback (callId + HMAC token)
  ↓
handleToolCallback()
  ↓ completeToolCallLog() (First-Wins: pending → terminal)
  ↓ afterToolCallTerminal() — 销毁 Pod
  ↓ 所有 pending 调用归零 → resumeSopFromAsyncTool() (L912)
  ↓ 重建 LLM 对话 → 重新执行挂起节点
```

---

## 6. 关键设计要点

### 状态持久化与断点恢复

SOP 执行状态通过 `stateSnapshot`（currentNodeId + nodeStates + executionPath + exitDecisions）持久化到数据库，支持：

- **断点恢复**：`paused_for_human`（人工审批）和 `paused_for_tool`（异步工具）状态可持久化数小时甚至跨天
- **冷恢复**：进程启动时 `recoverSopInstances()` (L1187) 查询未完成的 SOP 实例，按状态恢复

### 同步/异步双模式

| 模式 | 函数 | 适用场景 |
|------|------|---------|
| 同步 | `executeLLMWithTools()` (L158) | 工具快速返回（API 工具、进程内执行） |
| 异步 | `runAsyncToolLoop()` (L853) | 工具耗时较长（容器执行），派发后挂起 SOP，HTTP 回调恢复 |

异步模式将工具调用从内存 Promise 改为 Redis 持久化 Job + HTTP 回调，**BFF 重启不丢失调用**。

### 竞态保护

- **状态转换**：用 `WHERE status = fromStatus` 乐观锁，防止并发转换
- **异步恢复**：`completeToolCallLog` 的 First-Wins 去重，防止重复回调
- **死锁防护**：`resumeIfToolsAlreadySettled()` (engine.ts L890) 防止 settle-before-suspend 死锁

### 工具结果文件处理

挂载型工具的输出文件通过 NFS sop-files 目录共享：

1. BFF 负责文件名冲突解决（`(N)` 后缀）
2. `download_url` 注入到工具结果
3. `processDeliverables` 将文件提升到对话持久存储

### 多轮工具调用限制

对话引擎 `runMessageLoop()` 最多 `MAX_TOOL_ROUNDS = 5` 轮工具调用，防止无限循环。SOP 内的 `executeLLMWithTools()` 有独立的轮次限制。

---

## 关键文件索引

```
apps/crewmeld/lib/
├── conversation/
│   ├── engine.ts              # 对话引擎主循环
│   ├── intent-router.ts       # 意图分类 + 工具定义构建
│   ├── sop-bridge.ts          # 对话触发 SOP 的桥接
│   └── sop-status.ts          # SOP 状态查询
├── sop/
│   ├── engine.ts              # SOP 状态机主循环
│   ├── node-executor.ts       # 节点类型分派器
│   ├── llm-tool-executor.ts   # SOP 内 LLM 工具调用循环
│   ├── scheduler.ts           # BullMQ 定时触发
│   └── tool-callback-handler.ts  # 异步工具回调处理
├── tools/
│   ├── api-tool-runner.ts     # API 工具三阶段执行
│   └── script-invoker.ts      # 脚本工具容器执行
└── connectors/
    ├── resolve-conn-env.ts    # 系统连接 → CONN_* 环境变量
    ├── types.ts               # 连接类型定义
    └── tester.ts              # 连接测试
```
