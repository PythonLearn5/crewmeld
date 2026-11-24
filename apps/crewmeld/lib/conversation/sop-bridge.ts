/**
 * SOP 桥接 — 从对话中触发 SOP 执行
 *
 * 执行策略：带超时等待
 * - 启动 SOP 引擎后最多等待 SOP_WAIT_TIMEOUT_MS 毫秒
 * - 在超时内完成 -> 返回实际执行结果（如生成的 SQL、数据）
 * - 暂停等待人工确认或未完成 -> 返回"已启动" + executionId
 */

import {
  conversations,
  db,
  SOP_TERMINAL_STATUSES,
  sopDefinitions,
  sopExecutions,
  user,
} from '@crewmeld/db'
import { createLogger } from '@crewmeld/logger'
import { and, eq, notInArray } from 'drizzle-orm'
import { generateExecutionId } from '@/lib/core/execution-id'
import { t } from '@/lib/core/server-i18n'
import { getBaseUrl } from '@/lib/core/utils/urls'
import { resolveChannelIdentity } from '@/lib/identity/channel-identity'
import type { ScopeIdentity } from '@/lib/identity/types'
import { executeSop, transitionStatus } from '@/lib/sop/engine'
import { processDeliverables } from '@/lib/sop/deliverables'
import {
  type ConversationAttachment,
  seedFromConversationIoToSopFiles,
} from '@/lib/sop/sop-files-workspace'
import { getSopTimeoutQueue } from '@/lib/sop/queue'
import { validateSopForExecution } from '@/lib/sop/validator'
import type { SopNode, SopSerializedEdge, SopStateSnapshot } from '@/types/sop'
import { notifyChannelOnSopCompletion } from './sop-completion-notifier'

const logger = createLogger('SopBridge')

/**
 * 同步宽限窗口（毫秒）：对话保持其（SSE）轮次开启的时长，期望 SOP 完成
 * 以便在轮次内交付结果（一次干净的 LLM 回复）。SOP 受 LLM 约束 — 一个节点
 * 在大模型上约运行 2 轮 LLM 加上 pod 启动，因此即使最快的 SOP 也要 15-20 秒；
 * 绑定的工具本身很少成为瓶颈。比这更短的窗口会将每个 SOP 推到异步路径
 * （先发"执行中"确认 + 稍后推送结果），因此默认值为 60 秒：典型的多节点 SOP
 * 在轮次内完成，只有真正耗时的（多轮、慢工具）才会超出并将交付交给引擎的
 * 完成通知器。连接是带有周期性进度事件的 SSE 流，因此在窗口期间保持连接
 * 并非空闲保持。可通过 CREWMELD_SOP_SYNC_GRACE_MS 覆盖。
 */
const SOP_SYNC_GRACE_MS =
  Number(process.env.CREWMELD_SOP_SYNC_GRACE_MS) > 0
    ? Number(process.env.CREWMELD_SOP_SYNC_GRACE_MS)
    : 60_000

/** 轮询间隔（毫秒） */
const POLL_INTERVAL_MS = 500

export interface SopBridgeResult {
  success: boolean
  executionId: string
  sopName: string
  /** 是否已获得最终结果（completed/failed/error），false 表示仍在异步执行中 */
  completed: boolean
  /** 执行完成时的输出摘要 */
  output?: string
  /** 执行过程中产生的附件（来自工具返回的 files 字段） */
  files?: Array<{ name: string; mimeType: string; base64: string }>
  /**
   * 由挂载型（needsFileMount）工具产生并从 sop/{execId}/outputs/ 复制到
   * 对话持久化前缀的附件。此处的键是会话级（长期有效）的，适合存储在
   * conversationMessages.metadata.files 中。
   */
  workspaceFiles?: ConversationAttachment[]
  errorMessage?: string
}

/**
 * 从对话中触发 SOP 执行
 *
 * 1. 校验 SOP 定义存在且处于活跃状态
 * 2. 写入 sop_executions（pending）
 * 3. 状态流转 -> running
 * 4. 启动 SOP 引擎并等待完成（带超时）
 * 5. 返回执行结果或异步状态
 */
export async function executeSopFromConversation(
  conversationId: string,
  employeeId: string,
  sopDefinitionId: string,
  triggerData: Record<string, unknown>,
  userId: string,
  onProgress?: (message: string) => void,
  isZh = true,
  /** 接收消息的连接凭据 — 用于针对正确的应用解析调用者身份（无系统默认回退）。 */
  channelConfig?: Record<string, unknown>,
  /**
   * 用户为此请求指向的文件名（由 LLM 作为 SOP 工具的 `input_files` 传递）。
   * 仅这些 conv-io 文件会被种子化到 SOP 工作区。为空/未定义时不额外种子化 —
   * 调用者应将当前消息的附件作为回退传入。
   */
  inputFiles?: string[]
): Promise<SopBridgeResult> {
  const executionId = generateExecutionId('sop')

  // 1. 查询 SOP 定义（含 nodes/edges，用于检查协作节点 + 执行前校验）
  const [sopDef] = await db
    .select({
      id: sopDefinitions.id,
      name: sopDefinitions.name,
      version: sopDefinitions.version,
      nodes: sopDefinitions.nodes,
      edges: sopDefinitions.edges,
      sopTimeoutMinutes: sopDefinitions.sopTimeoutMinutes,
    })
    .from(sopDefinitions)
    .where(and(eq(sopDefinitions.id, sopDefinitionId), eq(sopDefinitions.isActive, true)))
    .limit(1)

  if (!sopDef) {
    return {
      success: false,
      executionId,
      sopName: '',
      completed: false,
      errorMessage: `SOP definition ${sopDefinitionId} not found or inactive`,
    }
  }

  // 执行前校验
  const validation = await validateSopForExecution(
    (sopDef.nodes ?? []) as SopNode[],
    (sopDef.edges ?? []) as SopSerializedEdge[]
  )
  if (!validation.valid) {
    const errorMessages = validation.errors.map((e) => `"${e.nodeName}" ${e.message}`).join('; ')
    return {
      success: false,
      executionId,
      sopName: sopDef.name,
      completed: false,
      errorMessage: `SOP validation failed: ${errorMessages}`,
    }
  }

  try {
    // 2. 写入执行记录
    //    元数据与用户参数分开存储，避免 source/conversationId/employeeId 污染工作流输入
    //    senderName 从对话元数据读取（由 webhook 异步写入）
    const [convRow] = await db
      .select({
        metadata: conversations.metadata,
        channel: conversations.channel,
        createdAt: conversations.createdAt,
      })
      .from(conversations)
      .where(eq(conversations.id, conversationId))
      .limit(1)
    // 定位对话的按日期分层的 conv-io 目录，用于交付物提升
    // （SOP NFS 输出 → 对话持久化存储）。
    const conversationCreatedAt = convRow?.createdAt ?? undefined
    let senderName = (convRow?.metadata as Record<string, unknown>)?.senderName as
      | string
      | undefined
    const channel = convRow?.channel ?? undefined

    // 回退：元数据中无 senderName 时（历史对话），从 user 表查询
    if (!senderName && userId) {
      const [userRow] = await db
        .select({ name: user.name })
        .from(user)
        .where(eq(user.id, userId))
        .limit(1)
      senderName = userRow?.name ?? undefined
    }
    logger.info('SOP bridge: reading sender name', {
      conversationId,
      senderName,
      metadata: convRow?.metadata,
    })

    // 从 IM 渠道解析调用者身份，使 node-executor 无需自行调用渠道 API
    // 即可读取 _meta.identity。
    let identityMeta: ScopeIdentity | undefined
    if (channel && userId) {
      try {
        identityMeta =
          (await resolveChannelIdentity({ channel, userId, config: channelConfig })) ?? undefined
      } catch (idErr) {
        logger.warn('SOP bridge: failed to resolve caller identity; proceeding without it', {
          conversationId,
          error: idErr instanceof Error ? idErr.message : String(idErr),
        })
      }

      // 始终附加 IM 级调用者信息（渠道 + 外部用户 ID + 发送者名称），
      // 确保 _meta.identity 对真实入站消息永不为空 — 即使对于解析为无
      // 内部员工的 web/匿名调用者也是如此。此基础层是当渠道未暴露组织
      // 范围时 forwardIdentity 转发的内容；已解析的渠道保留其字段并仅
      // 增加该 profile。
      identityMeta = {
        positions: [],
        scope: { orgUnitIds: [] },
        ...identityMeta,
        employeeId: identityMeta?.employeeId ?? userId,
        profile: { channel, externalUserId: userId, ...(senderName ? { senderName } : {}) },
      }
    }

    // 尝试获取 baseUrl，用于审批通知链接构造
    let baseUrl: string | undefined
    try {
      baseUrl = getBaseUrl()
    } catch {
      /* 非关键路径，忽略 */
    }

    await db.insert(sopExecutions).values({
      id: executionId,
      sopDefinitionId: sopDef.id,
      sopVersion: sopDef.version,
      triggeredBy: userId,
      status: 'pending',
      triggerData: {
        _meta: {
          source: 'conversation',
          conversationId,
          employeeId,
          senderName,
          senderEmail: userId,
          userId,
          channel,
          baseUrl,
          userLanguage: isZh ? 'zh' : 'en',
          ...(identityMeta ? { identity: identityMeta } : {}),
        },
        ...triggerData,
      },
    })

    // NFS sop-files 目录是惰性创建的，不在此处：下面的 conv-io 种子化
    // 仅在有输入文件需要复制时才 mkdirs，而 materializeMountedTools 在
    // 节点有 needsFileMount 工具可能写入输出时（对任何触发类型）mkdirs。
    // 因此无文件的 SOP 永不分配（空的）按执行实例的目录。

    // 从对话的 conv-io 目录种子化 SOP 的 NFS sop-files 工作区，
    // 使 dev-studio / opensandbox 工具能在 /root/io/<sopExecId>/ 看到
    // 用户上传的文件。尽力而为：复制失败仅记录警告但不阻止执行 —
    // 不读取附件的工具不受影响。
    try {
      // NFS conv-io → NFS sop-files 种子化，面向 dev-studio 工具
      // （kind=service deploy 或 kind=script ephemeral）。限制为 `inputFiles` —
      // 用户为此请求指向的文件（之前消息中发送过的模板仍可按名称找到；
      // 先前的输出/无关上传不会被灌入）。源目录为空或无匹配文件时为空操作。
      // 对应 dev-studio 测试流程中 lib/dev-studio/io-sync.ts 的
      // session-io → sop-files 复制。
      const [convRow] = await db
        .select({ createdAt: conversations.createdAt })
        .from(conversations)
        .where(eq(conversations.id, conversationId))
        .limit(1)
      if (convRow) {
        await seedFromConversationIoToSopFiles(
          conversationId,
          convRow.createdAt,
          executionId,
          inputFiles
        )
      }
    } catch (copyErr) {
      logger.warn('Failed to seed SOP inputs from conversation', {
        conversationId,
        executionId,
        error: copyErr instanceof Error ? copyErr.message : String(copyErr),
      })
    }

    // 2b. 注册 SOP 级超时任务（非阻塞，不得阻止执行）
    try {
      const timeoutQueue = getSopTimeoutQueue()
      if (timeoutQueue && sopDef.sopTimeoutMinutes > 0) {
        await timeoutQueue.add(
          'sop-timeout',
          {
            executionId,
            type: 'sop',
          },
          { delay: sopDef.sopTimeoutMinutes * 60 * 1000 }
        )
      }
    } catch (e) {
      logger.warn('Failed to register SOP timeout job, execution will proceed without timeout', {
        executionId,
        error: (e as Error).message,
      })
    }

    // 3. pending → running
    const transitioned = await transitionStatus(executionId, 'pending', 'running', {
      startedAt: new Date(),
    })

    if (!transitioned) {
      return {
        success: false,
        executionId,
        sopName: sopDef.name,
        completed: false,
        errorMessage: 'SOP status transition failed',
      }
    }

    // 此 SOP 从对话触发，将在同步宽限窗口内轮次内交付结果，因此抑制引擎的
    // 完成推送（默认 true）— 除非后续移交（宽限到期/暂停），届时会翻回。
    // 在引擎可能完成之前设置（下一行）。
    await db
      .update(sopExecutions)
      .set({ pushByEngine: false })
      .where(eq(sopExecutions.id, executionId))

    // 4. 启动 SOP 引擎（不 await，但跟踪 Promise）
    const sopPromise = executeSop(executionId).catch((error) => {
      logger.error(`SOP execution error: ${executionId}`, error)
    })

    // 检查 SOP 是否包含协作节点（human_employee / human_confirm）
    const sopNodes = (sopDef.nodes ?? []) as Array<{ type: string }>
    const hasCollaborator = sopNodes.some(
      (n) => n.type === 'human_employee' || n.type === 'human_confirm'
    )

    logger.info(
      `SOP triggered: execution=${executionId}, sop=${sopDef.name}, conversation=${conversationId}, hasCollaborator=${hasCollaborator}`
    )

    // 5. 等待 SOP 完成或超时
    //    有协作节点 -> 在 paused_for_human 时立即返回
    //    无协作节点 -> 最多等待 180 秒
    let result = await waitForCompletion(
      executionId,
      sopPromise,
      onProgress,
      hasCollaborator,
      isZh
    )

    // 未在宽限窗口内轮次内交付：将结果交付移交给引擎的完成通知器，
    // 将 pushByEngine 翻回开启。`status NOT terminal` 守卫使此操作对在
    // 轮询间隙刚到达终态的 SOP 是原子的 — 如果已完成，翻转不匹配任何记录，
    // 我们仍轮次内交付（引擎保持关闭）。
    if (!result.completed) {
      const handed = await db
        .update(sopExecutions)
        .set({ pushByEngine: true })
        .where(
          and(
            eq(sopExecutions.id, executionId),
            eq(sopExecutions.pushByEngine, false),
            notInArray(sopExecutions.status, SOP_TERMINAL_STATUSES)
          )
        )
        .returning({ id: sopExecutions.id })

      if (handed.length === 0) {
        const final = await queryExecutionResult(executionId)
        if (final.completed) result = final
      }
    }

    if (result.completed) {
      logger.info(`SOP completed synchronously: execution=${executionId}, status=${result.status}`)

      // SOP 结束时的交付物流程（仅成功路径）：
      // 提取 LLM 在最终消息中包含的 /api/sop/{execId}/files/... URL，
      // 将这些文件复制到 conversations/{convId}/，并重写 URL 指向
      // 长期有效的对话代理。纯 MinIO 操作 — 工具 pod 已在逐调用同步中
      // 将输出推送到 MinIO sop/{execId}/。BFF 不触及 PVC。
      let workspaceFiles: ConversationAttachment[] | undefined
      let finalOutput = result.output
      if (result.status === 'completed') {
        try {
          const deliverables = await processDeliverables({
            sopExecutionId: executionId,
            conversationId,
            conversationCreatedAt,
            finalMessage: result.output ?? '',
          })
          if (deliverables.attachments.length > 0) {
            workspaceFiles = deliverables.attachments
          }
          finalOutput = deliverables.message
        } catch (err) {
          logger.warn('SOP deliverables processing failed', {
            executionId,
            conversationId,
            error: err instanceof Error ? err.message : String(err),
          })
        }
      }

      return {
        success: result.status === 'completed',
        executionId,
        sopName: sopDef.name,
        completed: true,
        output: finalOutput,
        files: result.files,
        workspaceFiles,
        errorMessage: result.status !== 'completed' ? result.errorMessage : undefined,
      }
    }

    // 超时或暂停 — 返回异步状态，注册完成回调
    logger.info(
      `SOP executing asynchronously: execution=${executionId}, currentStatus=${result.status}`
    )

    // SOP 最终完成时，异步推送结果到 IM 渠道（飞书/企业微信）
    sopPromise
      .then(async () => {
        const finalResult = await queryExecutionResult(executionId)
        if (finalResult.completed) {
          // 镜像同步路径的交付物流程，使通知器看到长期有效的 URL。
          // 工具 pod 已通过逐调用同步将输出推送到 MinIO；BFF 仅执行
          // MinIO CopyObject 步骤。
          let workspaceFiles: ConversationAttachment[] | undefined
          let notifyOutput = finalResult.output
          if (finalResult.status === 'completed') {
            try {
              const deliverables = await processDeliverables({
                sopExecutionId: executionId,
                conversationId,
                conversationCreatedAt,
                finalMessage: finalResult.output ?? '',
              })
              if (deliverables.attachments.length > 0) {
                workspaceFiles = deliverables.attachments
              }
              notifyOutput = deliverables.message
            } catch (err) {
              logger.warn('SOP deliverables processing failed (async)', {
                executionId,
                conversationId,
                error: err instanceof Error ? err.message : String(err),
              })
            }
          }

          await notifyChannelOnSopCompletion({
            conversationId,
            sopName: sopDef.name,
            executionId,
            output: notifyOutput,
            files: finalResult.files,
            workspaceFiles,
            isZh,
            errorMessage: finalResult.errorMessage,
            status: finalResult.status as 'completed' | 'failed' | 'error',
          })
        }
      })
      .catch((error) => {
        logger.error(`SOP completion callback error: ${executionId}`, error)
      })

    return {
      success: true,
      executionId,
      sopName: sopDef.name,
      completed: false,
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    logger.error(`SOP bridge error: ${executionId}`, error)

    return {
      success: false,
      executionId,
      sopName: sopDef.name,
      completed: false,
      errorMessage,
    }
  }
}

interface FileAttachment {
  name: string
  mimeType: string
  base64: string
}

interface WaitResult {
  completed: boolean
  status: string
  output?: string
  files?: FileAttachment[]
  errorMessage?: string
}

/** 进度推送间隔（毫秒）— 无需每 500ms 推送，每 3 秒一次即可 */
const PROGRESS_INTERVAL_MS = 3_000

/**
 * 等待 SOP 执行完成（带超时 + 进度推送）
 *
 * 策略：轮询 SOP 执行状态，同时向前端推送步骤进度。
 * sopPromise 先完成或到达超时时结束等待。
 */
async function waitForCompletion(
  executionId: string,
  sopPromise: Promise<void>,
  onProgress?: (message: string) => void,
  hasCollaborator?: boolean,
  isZh = true
): Promise<WaitResult> {
  let done = false
  let lastProgressTime = 0
  let lastProgressMsg = ''

  // 监听 sopPromise 完成
  sopPromise
    .then(() => {
      done = true
    })
    .catch(() => {
      done = true
    })

  const startTime = Date.now()

  // `!done` 守卫刻意不在循环条件中：异步工具挂起会 resolve sopPromise，
  // 但 SOP 可能在等待窗口内通过回调恢复并完成，因此必须持续轮询。
  // 其他已 resolve 的状态在下方显式处理。
  while (Date.now() - startTime < SOP_SYNC_GRACE_MS) {
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS))

    // 检查是否到达终态
    const current = await queryExecutionResult(executionId)
    if (current.completed) {
      return current
    }

    // 有协作节点的 SOP：检测到暂停等待人工确认时立即返回，停止等待
    if (hasCollaborator && current.status === 'paused_for_human') {
      logger.info(
        `SOP entered human confirmation phase, returning immediately: execution=${executionId}`
      )
      return { completed: false, status: current.status }
    }

    // sopPromise 已 resolve 但执行仍非终态。`running`、`pending` 和
    // `paused_for_tool` 都是活跃状态 — 快速的异步工具回调可在本窗口内
    // 将 paused_for_tool→running→completed 翻转，因此必须持续轮询
    // （过快的回调曾在此被误读为"在另一状态下已 resolve"而提前退出，
    // 即使 SOP 片刻后完成也发出虚假的"仍在处理"）。只有人工暂停不会
    // 自行恢复，因此那是提前停止的唯一理由。
    if (done && current.status === 'paused_for_human') {
      return current
    }

    // 推送步骤进度（每 PROGRESS_INTERVAL_MS 一次）
    if (onProgress && Date.now() - lastProgressTime >= PROGRESS_INTERVAL_MS) {
      const progressMsg = buildSopProgressMessage(current.status, executionId, isZh)
      if (progressMsg && progressMsg !== lastProgressMsg) {
        onProgress(progressMsg)
        lastProgressMsg = progressMsg
      }
      lastProgressTime = Date.now()
    }
  }

  // 最终检查
  if (done) {
    return queryExecutionResult(executionId)
  }

  // 超时
  const current = await queryExecutionResult(executionId)
  if (current.completed) {
    return current
  }

  return { completed: false, status: current.status }
}

/**
 * 根据 SOP 执行状态构建进度提示文本
 */
function buildSopProgressMessage(status: string, _executionId: string, isZh = true): string | null {
  switch (status) {
    case 'running':
      return t('taskRunning', isZh ? 'zh' : 'en')
    case 'paused':
      return t('taskAwaitingConfirmation', isZh ? 'zh' : 'en')
    case 'pending':
      return t('taskQueued', isZh ? 'zh' : 'en')
    default:
      return null
  }
}

/**
 * 查询 SOP 执行最终结果
 */
async function queryExecutionResult(executionId: string): Promise<WaitResult> {
  const [exec] = await db
    .select({
      status: sopExecutions.status,
      stateSnapshot: sopExecutions.stateSnapshot,
      errorMessage: sopExecutions.errorMessage,
    })
    .from(sopExecutions)
    .where(eq(sopExecutions.id, executionId))
    .limit(1)

  if (!exec) {
    return { completed: true, status: 'error', errorMessage: 'Execution record not found' }
  }

  const terminalStatuses = ['completed', 'failed', 'timed_out', 'cancelled', 'error']
  const isTerminal = terminalStatuses.includes(exec.status)

  if (!isTerminal) {
    return { completed: false, status: exec.status }
  }

  // 从 stateSnapshot 提取最终输出和附件
  const snapshot = exec.stateSnapshot as unknown as SopStateSnapshot
  const output = extractOutputFromSnapshot(snapshot)
  const files = extractFilesFromSnapshot(snapshot)

  return {
    completed: true,
    status: exec.status,
    output,
    files: files.length > 0 ? files : undefined,
    errorMessage: exec.errorMessage ?? undefined,
  }
}

/**
 * 从状态快照提取执行输出摘要
 *
 * 按 executionPath 顺序遍历已完成节点的输出，拼接为可读文本。
 */
function extractOutputFromSnapshot(snapshot: SopStateSnapshot | null): string | undefined {
  if (!snapshot) return undefined

  const outputs: string[] = []

  // 按 execution path 顺序提取每个节点输出
  for (const nodeId of snapshot.executionPath) {
    const nodeState = snapshot.nodeStates[nodeId]
    if (nodeState?.status === 'completed' && nodeState.output) {
      const outputStr = formatNodeOutput(nodeState.output)
      if (outputStr) {
        outputs.push(outputStr)
      }
    }
  }

  // 同时检查 workflowResults
  for (const [_wfId, wfResult] of Object.entries(snapshot.workflowResults ?? {})) {
    if (wfResult && typeof wfResult === 'object') {
      const outputStr = formatNodeOutput(wfResult as unknown as Record<string, unknown>)
      if (outputStr) {
        outputs.push(outputStr)
      }
    }
  }

  if (outputs.length === 0) return undefined

  // 去重并合并
  const unique = [...new Set(outputs)]
  return unique.join('\n')
}

/**
 * 将单个节点输出格式化为可读字符串
 */
function formatNodeOutput(output: Record<string, unknown>): string | undefined {
  if (!output || Object.keys(output).length === 0) return undefined

  // 如果输出含有 result / output / content / text / sql 等常见字段，优先使用
  for (const key of ['result', 'output', 'content', 'text', 'sql', 'summary', 'response']) {
    if (output[key] && typeof output[key] === 'string') {
      return output[key] as string
    }
  }

  // 否则 JSON 序列化
  try {
    return JSON.stringify(output, null, 2)
  } catch {
    return undefined
  }
}

/**
 * 从状态快照中所有节点输出提取全部附件
 *
 * 工具执行结果存储在 nodeState.output.toolResults[].output 中，
 * 提取工具返回的 files 字段。
 */
function extractFilesFromSnapshot(snapshot: SopStateSnapshot | null): FileAttachment[] {
  if (!snapshot) return []

  const files: FileAttachment[] = []

  for (const nodeId of snapshot.executionPath) {
    const nodeState = snapshot.nodeStates[nodeId]
    if (nodeState?.status !== 'completed' || !nodeState.output) continue

    // toolResults 来自 LLM 工具执行结果
    const toolResults = nodeState.output.toolResults as
      | Array<{
          output?: unknown
        }>
      | undefined

    if (!Array.isArray(toolResults)) continue

    for (const tr of toolResults) {
      const output = tr.output as Record<string, unknown> | null
      if (!output) continue

      // 工具直接返回了 files 字段
      const resultFiles = output.files as
        | Array<{ name: string; mimeType: string; base64: string }>
        | undefined
      if (Array.isArray(resultFiles)) {
        for (const f of resultFiles) {
          if (f.name && f.base64) {
            files.push({
              name: f.name,
              mimeType: f.mimeType ?? 'application/octet-stream',
              base64: f.base64,
            })
          }
        }
      }
    }
  }

  return files
}
