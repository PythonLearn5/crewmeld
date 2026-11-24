/**
 * 对话引擎 —— 核心消息循环，自建 OpenAI 兼容 API 调用
 *
 * 流程：
 * 1. 加载会话 + 员工 -> 构建系统提示词 -> 构建上下文窗口
 * 2. 解析模型凭据 -> 构建工具
 * 3. fetch() 调用 OpenAI 兼容 /chat/completions（stream: true）
 * 4. 解析 SSE 响应，检测 tool_calls
 * 5. tool_call 匹配 wf_* -> 调用 workflow-bridge
 * 5b. tool_call 匹配 sop_* -> 调用 sop-bridge（非阻塞）
 * 6. 将工具结果注入 messages，重新调用 LLM
 * 7. 循环直到返回纯文本 -> 流式输出到前端
 */

/**
 * work_logs.content 的 i18n 策略（在计划 T11 中确定）：
 *   仅由 UI 消费 —— 所有读取路径（employees/[id]/logs、tasks/[id]/logs）均为
 *   返回 JSON 给前端的 API 路由；没有任何代码路径会将 work_logs.content 读回
 *   到 LLM messages 数组或任何提示字符串中。
 *
 * 因此：content 字段以 'en' 渲染（T12 将切换 lang→'en' 并添加 metadata.i18nKey）
 */

import {
  conversationMessages,
  conversations,
  db,
  digitalEmployees,
  sopExecutions,
  taskExecutions,
  workLogs,
} from '@crewmeld/db'
import { createLogger } from '@crewmeld/logger'
import { and, eq, inArray, sql } from 'drizzle-orm'
import { detect } from 'tinyld'
import { v4 as uuidv4 } from 'uuid'
import { CodedError } from '@/lib/core/errors'
import { t } from '@/lib/core/server-i18n'
import { encodeSSE } from '@/lib/core/utils/sse'
import { makeLogMetadata } from '@/lib/i18n/log-payload'
import { captureQuestionOccurrence } from '@/lib/knowledge/analytics/repository'
import {
  isModelDeclinedAnswer,
  recordUnansweredQuestionSafely,
  type UnansweredReason,
} from '@/lib/knowledge/unanswered/service'
import { buildContextWindow, resolveContextConfig, stripToolStructureFromHistory } from './context'
import { classifyIntent } from './intent-classifier'
import { resolveConversationIdentity } from './identity-resolution'
import { buildWorkflowToolConfigs } from './intent-router'
import { appendMissingKnowledgeImages } from './knowledge-images'
import { getEmployeeKnowledgeBaseIds, queryEmployeeKnowledge } from './knowledge-query'
import { mergeExtraParams, resolveModelConfig } from './model-config'
import { buildSystemPrompt } from './persona'
import { createConversationSseEmitter } from './runtime/conversation-sse'
import {
  createConversationRuntimeBoundary,
  measureRuntimeOperation,
  runConversationStreamStart,
} from './runtime/engine-orchestration'
import { prepareConversationRuntime } from './runtime/preparation'
import { createRuntimeTiming, type RuntimeTiming } from './runtime/timing'
import { executeSopFromConversation } from './sop-bridge'
import { extractSopExecutionIds, findUnverifiedActiveSopClaims } from './sop-claim-guard'
import type {
  ChatCompletionChunk,
  ConversationEvent,
  EngineMessage,
  KnowledgeChunkReference,
  OpenAITool,
  ToolCall,
} from './types'

async function executeWorkflowFromConversation(
  _conversationId: string,
  _employeeId: string,
  _workflowId: string,
  _args: Record<string, unknown>,
  _userId: string,
  _workspaceId: string
): Promise<{ success: boolean; outputSummary?: string; errorMessage?: string }> {
  return { success: false, errorMessage: 'Workflow execution is not supported in this version.' }
}

import { logModelUsage } from '@/lib/models/usage-logger'
import {
  buildAttachmentInstruction,
  getCustomerServiceConfig,
  renderGreeting,
} from './customer-service'
import { type FileAttachment, uploadConversationFile } from './file-storage'
import { prepareOcrContext } from './ocr-preparation'
import { querySopExecutionStatus } from './sop-status'
import { queryUserTasks, type TaskFilter } from './task-query'

const logger = createLogger('ConversationEngine')

const MAX_TOOL_ROUNDS = 5

/**
 * 标记在历史助手回复上的前缀，表示该回复确实来源于真实的 SOP 触发或状态查询。
 * 防止模型将有依据的确认回复当作普通聊天模式而在后续模仿。
 */
const SOP_DERIVED_MARKER = '[Grounded in a real tool result] '

function isSopDerived(metadata: unknown): boolean {
  return (
    typeof metadata === 'object' &&
    metadata !== null &&
    (metadata as Record<string, unknown>).sopDerived === true
  )
}

/**
 * 处理用户消息 —— 返回 SSE 流
 * @param preferredLocale - 前端传入的语言偏好（如 'zh-CN' 或 'en'），
 *   仅在自动检测结果不确定时作为回退，不会覆盖已明确检测到的语言。
 */
export async function processMessage(
  conversationId: string,
  userMessage: string,
  userId: string,
  /** 用户消息附带文件（已上传至 MinIO，写入 metadata.files） */
  fileMetadata?: Array<{ key: string; name: string; size: number; mimeType: string }>,
  /** 前端语言偏好，自动检测不确定时作为回退 */
  preferredLocale?: string,
  /** 接收消息的渠道连接凭据（IM webhook 场景），传递给 SOP 身份解析以确保使用正确的应用而非系统默认值 */
  channelConfig?: Record<string, unknown>,
  /** 接收消息的渠道连接 ID；web/api 渠道为 null/undefined */
  connectionId?: string,
  knowledgeBaseIds?: string[]
): Promise<ReadableStream<Uint8Array>> {
  // ═══ 编排主流程 ═══
  // ① 加载会话 & 数字员工配置
  // ② 保存用户消息（含文件附件、OCR、问题分析）
  // ③ 加载历史消息 & 过期工具结果清理（>5min 视为过期）
  // ④ 语言检测（CJK 字符集优先，拉丁语需≥8字符）
  // ⑤ 创建 SSE 流，进入实时编排：
  //    a. 纯文件短路 → 直接回复"请说明需求"
  //    b. 解析调用方身份 & 加载模型/知识库/工具配置
  //    c. 意图分类（knowledge_only / knowledge_then_sop / sop / direct）
  //    d. 知识库检索（RAGFlow，按意图决定是否检索）
  //    e. 构建系统提示词 & 上下文窗口
  //    f. 消息循环（多轮 tool_call，直到 LLM 不再调用工具）
  //    g. 记录执行结果 & 工作日志

  // 创建运行时计时器，追踪编排各阶段（加载/意图分类/检索/生成）的耗时
  const timing = createRuntimeTiming(uuidv4())
  // 创建运行时边界，包裹整个对话流程；完成时自动输出计时快照日志
  const runtimeBoundary = createConversationRuntimeBoundary(timing, (snapshot) =>
    logger.info('Conversation runtime completed', snapshot)
  )
  // 打起始时间戳，供后续 timing.measure() 计算各阶段耗时
  const conversationLoadStart = timing.mark('request')

  return runtimeBoundary.load(async () => {
    // ① 加载会话 —— 校验状态，已关闭的会话自动重新激活
    const [conv] = await db
      .select()
      .from(conversations)
      .where(eq(conversations.id, conversationId))
      .limit(1)

    if (!conv) {
      throw new CodedError('CONVERSATION_NOT_FOUND', t('conversationNotFound'))
    }

    if (conv.status !== 'active') {
      // 渠道消息到达时自动重新激活已关闭的会话
      if (conv.status === 'closed' || conv.status === 'archived') {
        await db
          .update(conversations)
          .set({ status: 'active', updatedAt: new Date() })
          .where(eq(conversations.id, conversationId))
        logger.info('Conversation reactivated', { conversationId, previousStatus: conv.status })
      } else {
        throw new CodedError('CONVERSATION_CLOSED', t('conversationClosed'))
      }
    }

    // ② 加载数字员工 —— 获取人格、配置、智能客服设定
    const [employee] = await db
      .select({
        id: digitalEmployees.id,
        name: digitalEmployees.name,
        description: digitalEmployees.description,
        persona: digitalEmployees.persona,
        config: digitalEmployees.config,
      })
      .from(digitalEmployees)
      .where(eq(digitalEmployees.id, conv.employeeId))
      .limit(1)

    if (!employee) {
      throw new Error(t('employeeNotFound'))
    }
    const customerService = getCustomerServiceConfig(employee.config)
    const greeting = customerService.greeting
      ? renderGreeting(customerService.greeting, { id: userId })
      : ''

    // ③ 保存用户消息 —— 处理文件附件（OCR + NFS 文件名）、问题分析采集、首次消息设为标题
    //    将附件信息作为结构化标注追加到消息末尾，供 LLM / SOP 工具识别文件
    const userMsgId = uuidv4()
    // 纯文件无文字内容：短路回复询问需求，跳过意图分类和 SOP
    // 渠道插件可能自动生成"用户发送了文件「xxx」"作为占位符，同样视为无用户需求
    const fileOnlyPlaceholder = /^(\[User sent a file:\s.+?\];?\s*)+$/i
    const hasFileOnly =
      fileMetadata &&
      fileMetadata.length > 0 &&
      (!userMessage.trim() || fileOnlyPlaceholder.test(userMessage.trim()))
    let ocrContext = ''
    try {
      ocrContext = await prepareOcrContext({
        message: userMessage,
        files: fileMetadata,
        employeeId: conv.employeeId,
        workspaceId: conv.workspaceId,
        employeeConfig: employee.config,
      })
    } catch (error) {
      logger.warn('OCR preparation failed', {
        conversationId,
        error: error instanceof Error ? error.message : String(error),
      })
    }
    const messageForUnderstanding = `${userMessage}${ocrContext}`
    let contentForLLM = messageForUnderstanding
    if (fileMetadata && fileMetadata.length > 0) {
      // 统一文件 IO 契约（规格 2026-06-01）：文件在任何工具运行前已预先放置在
      // NFS 的 `/root/io/<_sopFileDir>/<name>` 路径。
      //
      // 仅输出文件名 —— 不输出 URL —— 这样 LLM 传递的是纯文件名，
      // 工具将其拼接到 `/root/io/<_sopFileDir>/` 挂载路径上。此前告诉 LLM URL
      // 会导致它将 URL 作为 `pdf_file` 传递，工具随后将其拼接为
      // `/root/io/<dir>/<URL>` → FileNotFoundError。
      //
      // 文件名必须与 conv-io 中的**磁盘文件名**一致（上传时在 file-storage.ts:67
      // 处已做清理 —— `[^a-zA-Z0-9._\-CJK]` → `_`）。FileAttachment.name 保留
      // 原始用户可见名称用于聊天 UI；此处重新应用相同正则，确保 LLM 告诉工具的
      // 是确切的文件系统文件名。
      const sanitizeForNfs = (name: string): string => name.replace(/[^a-zA-Z0-9._\-一-鿿]/g, '_')
      const fileAnnotations = fileMetadata.map((f) => {
        const safeName = sanitizeForNfs(f.name)
        return `[附件: name=${safeName}, mimeType=${f.mimeType}, size=${f.size}]`
      })
      contentForLLM = `${messageForUnderstanding}\n\n${fileAnnotations.join('\n')}`
      contentForLLM += buildAttachmentInstruction(
        fileMetadata.map((file) => file.name),
        Boolean(userMessage.trim()) && !hasFileOnly
      )
    }
    await db.insert(conversationMessages).values({
      id: userMsgId,
      conversationId,
      role: 'user',
      content: contentForLLM,
      metadata: fileMetadata && fileMetadata.length > 0 ? { files: fileMetadata } : undefined,
    })
    if (userMessage.trim()) {
      try {
        await captureQuestionOccurrence({
          employeeId: conv.employeeId,
          conversationId,
          messageId: userMsgId,
          question: userMessage,
          requestedDatasetIds: knowledgeBaseIds,
        })
      } catch (error) {
        logger.warn('Question analytics capture failed', {
          conversationId,
          error: error instanceof Error ? error.message : String(error),
        })
      }
    }

    if (!conv.title) {
      await db
        .update(conversations)
        .set({ title: userMessage.slice(0, 50) })
        .where(eq(conversations.id, conversationId))
    }

    // ④ 加载历史消息 & 过期工具结果清理 —— 超过 5 分钟的 tool 结果标记为过期，防止 LLM 复用旧数据
    const historyRows = await db
      .select({
        role: conversationMessages.role,
        content: conversationMessages.content,
        toolCalls: conversationMessages.toolCalls,
        toolCallId: conversationMessages.toolCallId,
        toolName: conversationMessages.toolName,
        metadata: conversationMessages.metadata,
        createdAt: conversationMessages.createdAt,
      })
      .from(conversationMessages)
      .where(eq(conversationMessages.conversationId, conversationId))
      .orderBy(conversationMessages.createdAt)

    // 过期超过 5 分钟的工具交互，防止 LLM 复用旧数据而跳过工具调用
    // 需要过期三种消息类型：assistant(tool_calls) -> tool(result) -> assistant(基于结果的回复)
    // 以数据库时间为基准，避免 JS/DB 时区不一致
    const [{ dbNow }] = await db.execute<{ dbNow: Date }>(sql`SELECT NOW() as "dbNow"`)
    const dbNowMs = new Date(dbNow).getTime()
    const TOOL_RESULT_EXPIRE_MS = 5 * 60 * 1000
    const EXPIRED_HINT = t('expiredHint', 'zh')

    // 步骤 1：标记哪些 tool_call_ids 已过期
    const expiredToolCallIds = new Set<string>()
    for (const row of historyRows) {
      if (row.role === 'tool' && row.toolCallId && row.createdAt) {
        const age = dbNowMs - new Date(row.createdAt).getTime()
        if (age > TOOL_RESULT_EXPIRE_MS) {
          expiredToolCallIds.add(row.toolCallId)
        }
      }
    }

    // 步骤 2：构建历史消息，替换所有已过期的工具交互链
    const rawMessages: EngineMessage[] = historyRows.map((row) => {
      // tool 消息已过期 -> 替换内容
      if (row.role === 'tool' && row.toolCallId && expiredToolCallIds.has(row.toolCallId)) {
        return {
          role: row.role as EngineMessage['role'],
          content: EXPIRED_HINT,
          tool_call_id: row.toolCallId,
          name: row.toolName ?? undefined,
        }
      }
      // 带有 tool_calls 的 assistant 且所有调用均已过期 -> 保留 tool_calls 结构但标记
      if (row.role === 'assistant' && row.toolCalls) {
        const calls = row.toolCalls as ToolCall[]
        const allExpired = calls.length > 0 && calls.every((tc) => expiredToolCallIds.has(tc.id))
        if (allExpired) {
          return {
            role: row.role as EngineMessage['role'],
            content: EXPIRED_HINT,
            tool_calls: calls,
          }
        }
      }
      return {
        role: row.role as EngineMessage['role'],
        content:
          row.role === 'assistant' && isSopDerived(row.metadata) && row.content
            ? `${SOP_DERIVED_MARKER}${row.content}`
            : row.content,
        tool_calls: row.toolCalls as ToolCall[] | undefined,
        tool_call_id: row.toolCallId ?? undefined,
        name: row.toolName ?? undefined,
      }
    })

    // 步骤 3：过期紧跟在已过期工具链之后的 assistant 纯文本回复（基于工具结果的摘要）
    // 消息顺序：... -> assistant(tool_calls) -> tool(result) -> assistant(summary) -> ...
    const expiredMessages: EngineMessage[] = rawMessages.map((msg, i) => {
      if (
        msg.role === 'assistant' &&
        !msg.tool_calls &&
        msg.content &&
        msg.content !== EXPIRED_HINT
      ) {
        // 向前查找：如果前一条消息是已过期的 tool 消息，则当前 assistant 也是基于过期数据的摘要
        for (let j = i - 1; j >= 0; j--) {
          const prev = rawMessages[j]
          if (prev.role === 'tool' && prev.content === EXPIRED_HINT) {
            return { ...msg, content: EXPIRED_HINT }
          }
          // 跳过连续的 tool 消息（多工具调用场景）
          if (prev.role !== 'tool') break
        }
      }
      return msg
    })

    // 步骤 4：从发送给 LLM 的历史中剥离原始工具调用结构（assistant tool_calls + tool 结果）。
    // 上面的过期处理已中和了过期摘要（使模型在数据过期时仍会重新调用工具）；
    // 此处仅移除结构化的工具调用载荷，否则会使模型倾向于将工具调用
    // —— 例如被权限过滤的 SOP —— 作为原始 JSON 回传给用户。
    // 数据库持久化和 UI（直接读取 conversation_messages）不受影响。
    const historyMessages: EngineMessage[] = stripToolStructureFromHistory(expiredMessages)
    timing.measure('conversationLoadMs', conversationLoadStart)

    // ④ 语言检测 —— CJK 字符集优先（可靠），拉丁语需≥8字符或回退到历史消息/前端偏好
    const LANGUAGE_LABELS: Record<string, string> = {
      zh: '简体中文',
      en: 'English',
      ja: '日本語',
      ko: '한국어',
      fr: 'Français',
      de: 'Deutsch',
      es: 'Español',
      pt: 'Português',
      ru: 'Русский',
      ar: 'العربية',
      th: 'ไทย',
      vi: 'Tiếng Việt',
    }
    const SHORT_PHRASE_MAP: Record<string, string> = {
      // English
      hello: 'en',
      hi: 'en',
      hey: 'en',
      thanks: 'en',
      'thank you': 'en',
      yes: 'en',
      no: 'en',
      ok: 'en',
      okay: 'en',
      please: 'en',
      help: 'en',
      good: 'en',
      bye: 'en',
      goodbye: 'en',
      'good morning': 'en',
      'good evening': 'en',
      'good night': 'en',
      // Chinese
      你好: 'zh',
      您好: 'zh',
      嗨: 'zh',
      谢谢: 'zh',
      好的: 'zh',
      是的: 'zh',
      不是: 'zh',
      帮忙: 'zh',
      再见: 'zh',
      早上好: 'zh',
      晚上好: 'zh',
      请问: 'zh',
      好: 'zh',
      行: 'zh',
      可以: 'zh',
      // Japanese
      こんにちは: 'ja',
      おはよう: 'ja',
      ありがとう: 'ja',
      はい: 'ja',
      いいえ: 'ja',
      すみません: 'ja',
      さようなら: 'ja',
      // 한국어
      안녕하세요: 'ko',
      감사합니다: 'ko',
      네: 'ko',
      아니요: 'ko',
      // Français
      bonjour: 'fr',
      merci: 'fr',
      salut: 'fr',
      oui: 'fr',
      non: 'fr',
      'au revoir': 'fr',
      // Deutsch
      hallo: 'de',
      danke: 'de',
      ja: 'de',
      nein: 'de',
      tschüss: 'de',
      bitte: 'de',
      // Español
      hola: 'es',
      gracias: 'es',
      sí: 'es',
      adiós: 'es',
      'por favor': 'es',
      'buenos días': 'es',
      // Português
      olá: 'pt',
      obrigado: 'pt',
      obrigada: 'pt',
      sim: 'pt',
      não: 'pt',
      tchau: 'pt',
      // Русский
      привет: 'ru',
      спасибо: 'ru',
      да: 'ru',
      нет: 'ru',
      пожалуйста: 'ru',
      // العربية
      مرحبا: 'ar',
      شكرا: 'ar',
      نعم: 'ar',
      لا: 'ar',
      // ไทย
      สวัสดี: 'th',
      ขอบคุณ: 'th',
      // Tiếng Việt
      'xin chào': 'vi',
      'cảm ơn': 'vi',
    }
    // CJK 语言使用字符集检测，短文本也可靠；拉丁语系使用统计方法，需要足够长度
    const CJK_RELIABLE_CODES = new Set(['zh', 'ja', 'ko', 'th', 'ar', 'ru'])

    const detectUserLanguage = (message: string, history: EngineMessage[]): string => {
      // 短路：任意 CJK 汉字 → 直接判定为 zh/ja/ko。
      // tinyld 会将短的混合脚本消息（如中文 + 少量拉丁词）误判为拉丁语，
      // 从而将状态文本路由到错误的语言。
      if (/\p{Script=Han}/u.test(message)) {
        const cjkCode = detect(message)
        if (cjkCode === 'ja' || cjkCode === 'ko') return LANGUAGE_LABELS[cjkCode]
        return LANGUAGE_LABELS.zh
      }
      const normalized = message.trim().toLowerCase()
      const phraseCode = SHORT_PHRASE_MAP[normalized]
      if (phraseCode && LANGUAGE_LABELS[phraseCode]) return LANGUAGE_LABELS[phraseCode]
      const code = detect(message)
      // 信任 CJK 及其他非拉丁语的短文本；拉丁语系需要 >= 8 个字符
      if (code && LANGUAGE_LABELS[code] && (CJK_RELIABLE_CODES.has(code) || message.length >= 8))
        return LANGUAGE_LABELS[code]
      const recentUserMessages = history
        .filter((m) => m.role === 'user' && m.content && m.content.length >= 8)
        .slice(-3)
      for (let i = recentUserMessages.length - 1; i >= 0; i--) {
        const histCode = detect(recentUserMessages[i].content!)
        if (histCode && LANGUAGE_LABELS[histCode]) return LANGUAGE_LABELS[histCode]
      }
      if (code && LANGUAGE_LABELS[code]) return LANGUAGE_LABELS[code]
      // 回退：使用前端语言偏好（如有），否则默认中文
      if (preferredLocale) {
        const localeCode = preferredLocale.startsWith('en') ? 'en' : preferredLocale.split('-')[0]
        if (LANGUAGE_LABELS[localeCode]) return LANGUAGE_LABELS[localeCode]
      }
      return LANGUAGE_LABELS.zh
    }

    const userLanguage = detectUserLanguage(userMessage, historyMessages)
    const isZh = userLanguage === '简体中文'
    const lang = isZh ? 'zh' : 'en'
    logger.info('Language detection', { userMessage: userMessage.slice(0, 50), userLanguage, lang })

    // 多语言进度文本
    const PROGRESS_MESSAGES = {
      queryingKnowledge: t('queryingKnowledge', lang),
      generating: t('generating', lang),
    }

    // ⑤ 创建 SSE 流 —— 后续步骤在流内执行，实时推送进度事件
    return new ReadableStream<Uint8Array>({
      async start(controller) {
        /** 安全入队 —— controller 关闭后静默跳过，避免崩溃 */
        const safeEnqueue = (chunk: Uint8Array) => {
          try {
            controller.enqueue(chunk)
          } catch {
            // controller 被消费方取消（如 SSE 超时），静默忽略
          }
        }
        const sse = createConversationSseEmitter(safeEnqueue)

        /** 推送进度提示 */
        const pushProgress = (message: string) => {
          sse.progress(message)
        }

        // 立即推送"思考中"，供前端显示（跟随用户输入语言）
        pushProgress(t('thinking', lang))

        const taskId = uuidv4()
        const taskStartTime = Date.now()
        await runConversationStreamStart({
          timing,
          initialize: async () => {
            await db.insert(taskExecutions).values({
              id: taskId,
              employeeId: conv.employeeId,
              triggerType: 'conversation',
              status: 'running',
              input: { conversationId, message: userMessage.slice(0, 500) },
              inputSummary: userMessage.slice(0, 200),
              startedAt: new Date(),
            })
          },
          execute: async (runtimeTiming) => {
            // ⑤a 纯文件短路：用户只发了文件没有文字，直接回复询问需求，跳过意图分类和 SOP
            if (hasFileOnly && !ocrContext) {
              const fileNames = fileMetadata!.map((f) => `「${f.name}」`).join('、')
              const replyContent = t('convFileReceived', lang, { files: fileNames })
              const assistantMsgId = uuidv4()
              await db.insert(conversationMessages).values({
                id: assistantMsgId,
                conversationId,
                role: 'assistant',
                content: replyContent,
              })
              await db
                .update(conversations)
                .set({
                  messageCount: sql`${conversations.messageCount} + 2`,
                  lastMessageAt: new Date(),
                  updatedAt: new Date(),
                })
                .where(eq(conversations.id, conversationId))

              sse.messageStart(0)
              sse.messageDelta(replyContent)
              sse.messageDone(assistantMsgId, 0, [])

              await db
                .update(taskExecutions)
                .set({
                  status: 'success',
                  outputSummary: replyContent.slice(0, 200),
                  tokensUsed: 0,
                  durationMs: Date.now() - taskStartTime,
                  completedAt: new Date(),
                })
                .where(eq(taskExecutions.id, taskId))

              logger.info('File-only short circuit: replied asking for requirements', {
                conversationId,
                fileNames,
              })
              return
            }

            // ⑤b 解析调用方身份 & 并行加载模型配置/知识库/工具配置（身份解析决定 SOP 可见性）
            // SOP bridge 复用同一缓存）。IM 渠道从其通讯录解析；
            // web 从平台用户 + RBAC 角色解析（受合成的 'web' 连接门控）。
            // 'api'（及其他非 IM 渠道）产生 null 身份和 null connectionId → 默认可见性。
            // 5-7. 同时启动独立的预读取。工具可见性加载仅在调用方身份解析后启动。
            const {
              toolConfigs: {
                tools,
                workflowMap,
                sopMap,
                skillMap,
                sopInfos,
                deniedSopInfos,
                deniedSopMap,
              },
              modelConfig,
              knowledgeBaseIds: kbIds,
            } = await prepareConversationRuntime({
              loadIdentity: async () => {
                const identityStart = runtimeTiming.mark('identity')
                const { identity, connectionId: identityConnectionId } =
                  await resolveConversationIdentity({
                    channel: conv.channel,
                    userId,
                    connectionId,
                    channelConfig,
                  })
                runtimeTiming.measure('identityMs', identityStart)
                return { identity, connectionId: identityConnectionId }
              },
              loadModelConfig: () => resolveModelConfig(conv.employeeId, conv.workspaceId),
              loadKnowledgeBaseIds: () => getEmployeeKnowledgeBaseIds(conv.employeeId),
              loadToolConfigs: (visibility) =>
                buildWorkflowToolConfigs(conv.employeeId, visibility),
            })
            const hasKnowledgeBase = kbIds.length > 0
            const hasTools = tools.length > 0
            const sopNames = sopInfos.map((s) => s.name)

            // ⑤c 意图分类 —— 决定走知识库检索、SOP 执行还是直接对话
            let intentResult = await measureRuntimeOperation(runtimeTiming, 'routeMs', () =>
              classifyIntent(messageForUnderstanding, historyMessages, modelConfig, {
                hasKnowledgeBase,
                hasTools,
                sopNames,
                workflowNames: [],
              })
            )
            if (customerService.customerService && hasKnowledgeBase) {
              intentResult = {
                intent: 'knowledge_only',
                reason: 'Customer service mode restricts responses to knowledge retrieval',
                searchQuery: messageForUnderstanding,
              }
            }

            logger.info('Intent classification result', {
              intent: intentResult.intent,
              reason: intentResult.reason,
            })

            // 推送意图分类结果到前端
            const intentEvent: ConversationEvent = {
              type: 'intent:classified',
              data: {
                intent: intentResult.intent,
                reason: intentResult.reason,
                hasKnowledgeReference: false,
              },
            }
            safeEnqueue(encodeSSE(intentEvent))

            // ⑤d 知识库检索 —— 按意图决定是否查询 RAGFlow（智能客服模式强制只走知识库）
            let knowledgeReference: string | null = null
            let knowledgeReferences: KnowledgeChunkReference[] = []
            let unansweredReason: UnansweredReason | null = null
            let unansweredSimilarity: number | null = null

            if (
              (intentResult.intent === 'knowledge_only' ||
                intentResult.intent === 'knowledge_then_sop') &&
              hasKnowledgeBase
            ) {
              pushProgress(PROGRESS_MESSAGES.queryingKnowledge)
              // 始终基于用户原始措辞检索。意图分类器会将信号词
              //（"...的参数"、"...怎么用"）剥离为关键词桩，导致过度召回
              // 产品概览分块，而非用户实际所需的参数/规格分块。
              const searchQuery = messageForUnderstanding
              const retrievalStart = runtimeTiming.mark('retrieval')
              const kbResult = await queryEmployeeKnowledge(
                conv.employeeId,
                searchQuery,
                8,
                knowledgeBaseIds,
                conv.channel === 'api'
              )
              runtimeTiming.measure('retrievalMs', retrievalStart)

              if (kbResult.success && kbResult.referenceText) {
                knowledgeReference = kbResult.referenceText
                knowledgeReferences = kbResult.references
                logger.info('Knowledge base search completed, reference info injected', {
                  resultCount: kbResult.resultCount,
                })
              }

              const kbEvent: ConversationEvent = {
                type: 'knowledge:result',
                data: { resultCount: knowledgeReferences.length },
              }
              safeEnqueue(encodeSSE(kbEvent))

              if (
                customerService.customerService &&
                customerService.trackUnansweredQuestions &&
                !kbResult.failureType &&
                userMessage.trim() &&
                !kbResult.success
              ) {
                unansweredReason = kbResult.resultCount === 0 ? 'no_chunks' : 'low_similarity'
                unansweredSimilarity = kbResult.maxSimilarity
              }
            }

            // ⑤e 确定工具集 —— 智能客服模式或 knowledge_only 意图时不带工具
            const effectiveTools =
              customerService.customerService || intentResult.intent === 'knowledge_only'
                ? []
                : tools

            // 10. 构建 SOP 名称映射（工具函数名 -> 显示名称）
            const sopNameMap = new Map<string, string>()
            for (const info of sopInfos) {
              sopNameMap.set(`sop_${info.id}`, info.name)
            }

            // ⑤f 构建系统提示词 —— 融合数字员工人格、SOP 列表、知识库引用、开场白
            let systemPrompt = buildSystemPrompt(
              employee,
              [],
              sopInfos,
              knowledgeReference,
              userLanguage,
              deniedSopInfos
            )
            if (customerService.customerService) {
              systemPrompt +=
                '\n\n## Customer Service Mode\n仅使用已检索的知识库内容回答，不执行任务流程或外部工具；没有可靠依据时请明确告知用户。'
            }
            // 检查历史消息中是否已发送过开场白（metadata.source === 'greeting'）
            const hasPersistedGreeting = historyRows.some(
              (row) =>
                row.metadata &&
                typeof row.metadata === 'object' &&
                (row.metadata as Record<string, unknown>).source === 'greeting'
            )
            // 仅在首次对话时注入开场白：有开场白配置 + 未发送过 + 消息数≤1
            if (greeting && !hasPersistedGreeting && historyRows.length <= 1) {
              systemPrompt += `\n\n## Opening Greeting\nBegin your first response with: ${greeting}`
            }

            logger.info('Tool list', {
              toolCount: effectiveTools.length,
              tools: effectiveTools.map((t) => ({
                name: t.function.name,
                description: t.function.description,
              })),
              intent: intentResult.intent,
            })
            logger.info('System prompt', { systemPrompt: systemPrompt.slice(0, 1000) })

            // ⑤g 构建上下文窗口 —— 按模型实际 context window 截断历史
            const contextMessages = buildContextWindow(
              historyMessages,
              systemPrompt,
              resolveContextConfig(modelConfig.model)
            )

            // ⑤h 消息循环 —— 多轮 tool_call，LLM 可调用工具/SOP，结果回传后继续生成，直到不再调用工具
            pushProgress(PROGRESS_MESSAGES.generating)

            const loopResult = await runMessageLoop(
              safeEnqueue,
              conversationId,
              conv.employeeId,
              userId,
              conv.workspaceId,
              systemPrompt,
              contextMessages,
              effectiveTools,
              workflowMap,
              sopMap,
              sopNameMap,
              deniedSopMap,
              skillMap,
              modelConfig,
              runtimeTiming,
              userMessage,
              knowledgeReferences,
              taskId,
              isZh,
              channelConfig,
              fileMetadata
            )
            if (
              customerService.customerService &&
              customerService.trackUnansweredQuestions &&
              userMessage.trim() &&
              (unansweredReason || isModelDeclinedAnswer(loopResult.outputSummary))
            ) {
              void recordUnansweredQuestionSafely({
                employeeId: conv.employeeId,
                conversationId,
                messageId: userMsgId,
                question: userMessage,
                knowledgeBaseId: null,
                maxSimilarity: unansweredSimilarity,
                reason: unansweredReason ?? 'model_declined',
              })
            }
            // ⑤i 记录执行结果 & 工作日志（智能客服模式额外记录未回答问题）
            await db
              .update(taskExecutions)
              .set({
                status: 'success',
                outputSummary: loopResult.outputSummary,
                tokensUsed: loopResult.totalTokens,
                durationMs: Date.now() - taskStartTime,
                completedAt: new Date(),
              })
              .where(eq(taskExecutions.id, taskId))
            await db.insert(workLogs).values({
              id: uuidv4(),
              taskId,
              employeeId: conv.employeeId,
              logType: 'action',
              content: t('conversationDone', 'en'),
              metadata: makeLogMetadata(
                { conversationId, tokensUsed: loopResult.totalTokens },
                { i18nKey: 'conversationDone', i18nParams: {} }
              ),
            })
          },
          onError: async (error) => {
            const msg = error instanceof Error ? error.message : t('unknownError', lang)
            logger.error('Conversation engine error', error)
            const event: ConversationEvent = { type: 'error', data: { message: msg } }
            safeEnqueue(encodeSSE(event))
            await db
              .update(taskExecutions)
              .set({
                status: 'failed',
                errorMessage: msg,
                durationMs: Date.now() - taskStartTime,
                completedAt: new Date(),
              })
              .where(eq(taskExecutions.id, taskId))
              .catch(() => {})
            await db
              .insert(workLogs)
              .values({
                id: uuidv4(),
                taskId,
                employeeId: conv.employeeId,
                logType: 'error',
                content: `${t('convProcessFailed', 'en')}: ${msg}`,
                metadata: makeLogMetadata(
                  { conversationId },
                  { i18nKey: 'convProcessFailed', i18nParams: { msg } }
                ),
              })
              .catch(() => {})
          },
          onComplete: () => {
            runtimeBoundary.complete()
            try {
              controller.close()
            } catch {
              /* 已关闭 */
            }
          },
        })
      },
    })
  })
}

interface ModelConfig {
  providerId: string
  model: string
  apiKey: string
  baseUrl: string
  /** 用户自定义的透传参数，合并到请求体中（如 enable_thinking）。 */
  extraParams?: Record<string, unknown>
}

/**
 * 消息循环 —— 支持多轮 tool_call
 */
async function runMessageLoop(
  enqueue: (chunk: Uint8Array) => void,
  conversationId: string,
  employeeId: string,
  userId: string,
  workspaceId: string,
  systemPrompt: string,
  contextMessages: EngineMessage[],
  tools: OpenAITool[],
  workflowMap: Map<string, string>,
  sopMap: Map<string, string>,
  sopNameMap: Map<string, string>,
  /** sopToolName（`sop_<id>`）→ 被拒绝的 SOP 描述符；对这些的调用在程序侧被拒绝。 */
  deniedSopMap: Map<string, { id: string; name: string }>,
  skillMap: Map<string, { skillId: string; endpoint: string; openclawConnectionId?: string }>,
  modelConfig: ModelConfig,
  timing: RuntimeTiming,
  userMessage: string,
  knowledgeReferences: KnowledgeChunkReference[] = [],
  taskId = '',
  isZh = true,
  /** 接收消息的连接凭据 —— 传递给 SOP 身份解析。 */
  channelConfig?: Record<string, unknown>,
  /** 当前用户消息附带的文件 —— 当 LLM 未指定 `input_files` 时的回退种子集。 */
  fileMetadata?: Array<{ key: string; name: string; size: number; mimeType: string }>
): Promise<{ outputSummary: string | null; totalTokens: number }> {
  const lang = isZh ? 'zh' : 'en'
  const messages: EngineMessage[] = [{ role: 'system', content: systemPrompt }, ...contextMessages]

  let totalTokens = 0

  /** 工具执行期间产生的文件（已上传至 MinIO），写入助手消息 metadata */
  const collectedFileMetadata: FileAttachment[] = []

  /** 本轮对话中已触发的 SOP ID 集合 —— 防止跨轮重复触发 */
  const triggeredSopIds = new Set<string>()

  /** 上一轮是否产生了有依据的 SOP 结果。 */
  let previousRoundHadSopOutput = false
  let firstTokenRecorded = false
  const conversationSse = createConversationSseEmitter(enqueue)

  for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
    // 通知前端消息开始
    const messageSse = conversationSse.startMessage(round)

    const llmStart = timing.mark('llm')
    const { content, toolCalls, usage } = await callLLM(
      messages,
      tools,
      modelConfig,
      messageSse.delta,
      () => {
        if (firstTokenRecorded) return
        firstTokenRecorded = true
        timing.measure('llmFirstTokenMs', llmStart)
      }
    )
    totalTokens += usage.total

    // 写入模型调用日志
    logModelUsage({
      provider: modelConfig.providerId,
      model: modelConfig.model,
      userId,
      employeeId,
      response: {
        content: content ?? '',
        model: modelConfig.model,
        tokens: { input: usage.input, output: usage.output, total: usage.total },
      },
    })
    if (taskId) {
      db.insert(workLogs)
        .values({
          id: uuidv4(),
          taskId,
          employeeId,
          logType: 'llm_call',
          content: t('convModelUsage', 'en', {
            model: modelConfig.model,
            tokens: String(usage.total),
          }),
          metadata: makeLogMetadata(
            {
              model: modelConfig.model,
              tokensInput: usage.input,
              tokensOutput: usage.output,
              round,
            },
            {
              i18nKey: 'convModelUsage',
              i18nParams: { model: modelConfig.model, tokens: usage.total },
            }
          ),
        })
        .catch(() => {})
    }

    // 纯文本回复 —— 保存并结束
    if (!toolCalls || toolCalls.length === 0) {
      const rawContent = content ?? ''
      const claimedIds = extractSopExecutionIds(rawContent)
      if (claimedIds.length > 0) {
        const existingRows = await db
          .select({ id: sopExecutions.id })
          .from(sopExecutions)
          .where(inArray(sopExecutions.id, claimedIds))
        const existingIds = new Set(existingRows.map((row) => row.id))
        const bogusIds = findUnverifiedActiveSopClaims(rawContent, existingIds)

        if (bogusIds.length > 0) {
          logger.warn('Blocked unverified SOP execution claim from LLM reply', {
            conversationId,
            employeeId,
            model: modelConfig.model,
            round,
            bogusIds,
          })
          messages.push({ role: 'assistant', content })
          messages.push({
            role: 'system',
            content: t('sopClaimUnverified', lang, { ids: bogusIds.join(', ') }),
          })
          continue
        }
      }

      const assistantMsgId = uuidv4()
      const finalContent = appendMissingKnowledgeImages(rawContent, knowledgeReferences)
      const appendedContent = finalContent.slice(rawContent.length)
      if (appendedContent) messageSse.delta(appendedContent)

      await db.insert(conversationMessages).values({
        id: assistantMsgId,
        conversationId,
        role: 'assistant',
        content: finalContent,
        tokensUsed: usage.total,
        metadata: {
          ...(knowledgeReferences.length > 0 ? { references: knowledgeReferences } : {}),
          ...(collectedFileMetadata.length > 0 ? { files: collectedFileMetadata } : {}),
          ...(previousRoundHadSopOutput ? { sopDerived: true } : {}),
        },
      })

      // Update conversation statistics
      await db
        .update(conversations)
        .set({
          messageCount: sql`${conversations.messageCount} + 2`,
          totalTokens: sql`${conversations.totalTokens} + ${totalTokens}`,
          lastMessageAt: new Date(),
          updatedAt: new Date(),
          title: sql`CASE WHEN ${conversations.title} IS NULL THEN ${finalContent.slice(0, 50)} ELSE ${conversations.title} END`,
        })
        .where(eq(conversations.id, conversationId))

      messageSse.done(assistantMsgId, totalTokens, knowledgeReferences)
      return { outputSummary: finalContent.slice(0, 200) || null, totalTokens }
    }

    // 有 tool_calls —— 保存助手消息
    // 丢弃 tool_call 轮次的内容，防止 LLM 在后续轮次看到已回复的内容而产生重复
    messageSse.toolOnly()
    const assistantToolMsgId = uuidv4()
    await db.insert(conversationMessages).values({
      id: assistantToolMsgId,
      conversationId,
      role: 'assistant',
      content: null,
      toolCalls,
      tokensUsed: usage.total,
    })

    messages.push({ role: 'assistant', content: null, tool_calls: toolCalls })

    // 处理每个 tool_call（每轮最多 1 个 SOP）
    let sopCalledThisRound = false
    let hasRealToolExecution = false
    let sopOutputThisRound = false
    for (const tc of toolCalls) {
      const toolDisplayName = tc.function.name.startsWith('sop_')
        ? t('sopLabel', lang)
        : tc.function.name
      const toolStartEvent: ConversationEvent = {
        type: 'tool:start',
        data: {
          toolCallId: tc.id,
          toolName: tc.function.name,
          displayMessage: t('executingTool', lang, { name: toolDisplayName }),
        },
      }
      enqueue(encodeSSE(toolStartEvent))
      let toolResult = ''
      let skipPersist = false // 被拦截的响应不保存到对话历史，避免刷新过期窗口
      await measureRuntimeOperation(timing, 'toolMs', async () => {
        const wfId = workflowMap.get(tc.function.name)
        const sopId = sopMap.get(tc.function.name)
        const skillInfo = skillMap.get(tc.function.name)
        const deniedSop = deniedSopMap.get(tc.function.name)

        let args: Record<string, unknown> = {}
        try {
          args = JSON.parse(tc.function.arguments)
        } catch {
          args = { raw: tc.function.arguments }
        }

        if (tc.function.name === 'check_sop_status') {
          // SOP 状态查询
          const executionId = (args.execution_id as string) ?? ''
          if (!executionId) {
            toolResult = t('missingExecutionId', lang)
          } else {
            const statusResult = await querySopExecutionStatus(executionId, lang)
            toolResult = statusResult.summary
            sopOutputThisRound = true
          }
        } else if (tc.function.name === 'query_my_tasks') {
          // 用户任务列表查询
          const filter = (args.filter as TaskFilter) ?? 'all'
          const limit = typeof args.limit === 'number' ? args.limit : 10
          const taskResult = await queryUserTasks(userId, filter, limit)
          toolResult = taskResult.summary
        } else if (deniedSop) {
          // Restricted task: caller lacks permission. Reject program-side without
          // executing; the message is returned so the LLM relays the refusal.
          // hasRealToolExecution stays false; no SOP dedup/trigger logic runs.
// 受限任务：调用方缺乏权限。在程序端直接拒绝，不予执行；将相关信息返回，以便由大语言模型（LLM）传达这一拒绝。
// hasRealToolExecution 保持为 false；不会执行任何 SOP 去重或触发逻辑。
          toolResult = t('sopNoPermission', lang, { name: deniedSop.name })
          logger.warn('Denied SOP invocation blocked program-side', {
            conversationId,
            tool: tc.function.name,
            sopId: deniedSop.id,
          })
        } else if (wfId) {
          // Workflow call
          hasRealToolExecution = true
          const result = await executeWorkflowFromConversation(
            conversationId,
            employeeId,
            wfId,
            (args.input as Record<string, unknown>) ?? args,
            userId,
            workspaceId
          )

          toolResult = result.success
            ? (result.outputSummary ?? t('executionSuccess', lang))
            : `${t('convExecFailed', lang)}: ${result.errorMessage}`
        } else if (sopId) {
          // Rate limit: max 1 SOP per tool_calls round; no duplicate trigger for same SOP across rounds; no duplicate trigger within 5 min across requests
          let recentlyTriggered = false
          if (!triggeredSopIds.has(sopId)) {
            // Check DB: same user + same SOP + same user message + triggered within last 5 min
            // Use database time NOW() for comparison, avoid JS/DB timezone inconsistency causing misjudgment
// 检查数据库：同一用户 + 同一 SOP + 同一用户消息 + 触发时间在过去 5 分钟内
// 使用数据库时间 NOW() 进行比对，避免因 JS 与数据库时区不一致而导致误判            
            const [recent] = await db
              .select({ id: sopExecutions.id })
              .from(sopExecutions)
              .where(
                and(
                  eq(sopExecutions.sopDefinitionId, sopId),
                  eq(sopExecutions.triggeredBy, userId),
                  sql`${sopExecutions.createdAt} > NOW() - INTERVAL '5 minutes'`,
                  sql`${sopExecutions.triggerData}->>'input' = ${userMessage}`
                )
              )
              .limit(1)
            recentlyTriggered = !!recent
            if (recentlyTriggered) {
              logger.warn(
                `SOP duplicate trigger within 5 minutes intercepted: sopId=${sopId}, conversation=${conversationId}, message=${userMessage.slice(0, 50)}`
              )
            }
          }

          if (sopCalledThisRound) {
            toolResult = t('taskSkipped', lang)
            skipPersist = true
            logger.warn(
              `SOP concurrent call intercepted: sopId=${sopId}, conversation=${conversationId}`
            )
          } else if (triggeredSopIds.has(sopId)) {
            toolResult = t('taskAlreadyTriggered', lang)
            skipPersist = true
            logger.warn(
              `SOP duplicate trigger intercepted: sopId=${sopId}, conversation=${conversationId}`
            )
          } else if (recentlyTriggered) {
            toolResult = t('taskRecentlyExecuted', lang)
            skipPersist = true
          } else {
            triggeredSopIds.add(sopId)
            sopCalledThisRound = true
            sopOutputThisRound = true

            // Push progress: inform user which SOP is being executed
            const sopDisplayName = sopNameMap.get(tc.function.name) ?? 'SOP'
            const sopProgressEvent: ConversationEvent = {
              type: 'progress',
              data: { message: t('executingTask', lang, { name: sopDisplayName }) },
            }
            enqueue(encodeSSE(sopProgressEvent))

            // SOP call (with timeout wait)
            // Inject original user message into input, ensure SOP start_trigger can access it.
            // LLM-extracted precise params (sopInput) expanded later; same-name fields override input.
            const sopInput = (args.input as Record<string, unknown>) ?? args
            // Files the user pointed at for THIS request. The outer LLM relays the
            // names it read in the user's message via `input_files`; only those
            // conv-io files get seeded into the SOP workspace (so a template sent
            // once earlier is picked up by name, and prior outputs / unrelated
            // uploads are not dumped in). Fall back to the files attached to the
            // current message when the LLM named none.
            const namedInputFiles = Array.isArray(args.input_files)
              ? (args.input_files as unknown[]).filter((x): x is string => typeof x === 'string')
              : []
            const inputFiles =
              namedInputFiles.length > 0 ? namedInputFiles : (fileMetadata ?? []).map((f) => f.name)
            // Collect [附件:] annotations from ALL user messages in history (not
            // just the most recent one), deduped by filename keeping the latest
            // upload. A single SOP trigger often follows several separate
            // file-upload messages (e.g. data Excel + PPT template sent one at a
            // time, each with its own "已收到" reply in between); taking only the
            // last file message would drop the earlier files and make the SOP's
            // inner LLM believe they are missing. This mirrors the SOP bridge's
            // conv-io → sop-files seed, which copies every conversation file.
            const seenAttachmentNames = new Set<string>()
            const attachmentAnnotations: string[] = []
            for (let i = messages.length - 1; i >= 0; i--) {
              const m = messages[i]
              if (m.role !== 'user' || typeof m.content !== 'string') continue
              const matches = m.content.match(/\[附件:[^\]]+\]/g)
              if (!matches) continue
              for (const annotation of matches) {
                const name = annotation.match(/name=([^,\]]+)/)?.[1]?.trim() ?? annotation
                if (seenAttachmentNames.has(name)) continue
                seenAttachmentNames.add(name)
                attachmentAnnotations.push(annotation)
              }
            }
            // Newest-first above → reverse to chronological order for the prompt.
            attachmentAnnotations.reverse()
            const sopInputMessage =
              attachmentAnnotations.length > 0
                ? `${userMessage}\n\n${attachmentAnnotations.join('\n')}`
                : userMessage
            const triggerData: Record<string, unknown> = {
              input: sopInputMessage,
              ...sopInput,
            }
            const sopDispatchStart = timing.mark('sopDispatch')
            let result: Awaited<ReturnType<typeof executeSopFromConversation>>
            try {
              result = await executeSopFromConversation(
                conversationId,
                employeeId,
                sopId,
                triggerData,
                userId,
                (progressMsg) => {
                  const progressEvent: ConversationEvent = {
                    type: 'progress',
                    data: { message: progressMsg },
                  }
                  enqueue(encodeSSE(progressEvent))
                },
                isZh,
                channelConfig,
                inputFiles
              )
            } finally {
              timing.measure('sopDispatchMs', sopDispatchStart)
            }

            if (!result.success) {
              toolResult = `${t('taskStartFailed', lang)}: ${result.errorMessage}`
            } else if (result.completed && result.output) {
              // SOP execution completed, return actual result
              let outputForLLM = result.output
              // If output contains files base64, clean up to prevent LLM from outputting raw encoding
              try {
                const parsed = JSON.parse(result.output)
                if (typeof parsed === 'object' && parsed !== null && Array.isArray(parsed.files)) {
                  const { files: _files, ...rest } = parsed
                  outputForLLM =
                    Object.keys(rest).length > 0
                      ? JSON.stringify(rest, null, 2)
                      : (parsed.result ?? t('executionDone', lang))
                }
              } catch {
                /* not JSON, use directly */
              }

              // Build human-facing "file generated" note from both sources:
              //   - result.files          : base64 from non-mount tools
              //   - result.workspaceFiles : already in conversations/, from mounted tools
              const fileNames = [
                ...(result.files?.map((f) => f.name) ?? []),
                ...(result.workspaceFiles?.map((f) => f.name) ?? []),
              ]
              const fileNoteText =
                fileNames.length > 0
                  ? `\n\n[${t('fileGenerated', lang)}: ${fileNames.join(', ')}. ${t('fileNote', lang)}]`
                  : ''
              toolResult = `${t('taskCompleted', lang, { name: result.sopName, id: result.executionId })}\n\n${outputForLLM}${fileNoteText}`

              // Surface attachments via SSE message:files event:
              //   - base64 files need uploadFilesToMinio (writes to conversations/)
              //   - workspaceFiles are already in conversations/ (sop-bridge
              //     copied them via CopyObject) — use directly
              const baseFiles =
                result.files && result.files.length > 0
                  ? await uploadFilesToMinio(conversationId, result.files, collectedFileMetadata)
                  : []
              // Match the shape uploadFilesToMinio returns so the SSE event
              // is consistent. Workspace files have no base64 payload.
              const workspaceFilesForSse = (result.workspaceFiles ?? []).map((f) => ({
                name: f.name,
                mimeType: f.mimeType,
                base64: '',
                key: f.key,
              }))
              if (result.workspaceFiles && result.workspaceFiles.length > 0) {
                collectedFileMetadata.push(...result.workspaceFiles)
              }
              const allFiles = [...baseFiles, ...workspaceFilesForSse]
              if (allFiles.length > 0) {
                const filesEvent: ConversationEvent = {
                  type: 'message:files',
                  data: { files: allFiles },
                }
                enqueue(encodeSSE(filesEvent))
              }
            } else if (result.completed && !result.output) {
              toolResult = t('taskCompletedNoOutput', lang, {
                name: result.sopName,
                id: result.executionId,
              })
            } else {
              // SOP still executing asynchronously (e.g. awaiting human confirmation)
              toolResult = t('taskStarted', lang, { name: result.sopName, id: result.executionId })
            }
          }
        } else if (skillInfo) {
          if (skillInfo.openclawConnectionId) {
            /**
             * OpenClaw async dispatch — fire-and-forget.
             * Return immediate ack to the LLM; real result is persisted to
             * `conversation_messages` + pushed via IM channel by the handler.
             */
            hasRealToolExecution = true
            const openclawTaskId = uuidv4()

            const { dispatchOpenclawAsync } = await import('@/lib/openclaw/async-handler')
            void dispatchOpenclawAsync({
              taskId: openclawTaskId,
              conversationId,
              connectionId: skillInfo.openclawConnectionId,
              args: {
                message: typeof args.message === 'string' ? args.message : '',
                ...(typeof args.model === 'string' && args.model !== ''
                  ? { model: args.model }
                  : {}),
              },
            })

            toolResult = JSON.stringify({
              status: 'pending',
              task_id: openclawTaskId,
              message: 'OpenClaw 已收到任务，正在处理中。结果将稍后追加到对话。',
            })
          } else {
            // Skill tool call — HTTP POST to deployed K8S endpoint
            try {
              const skillResponse = await fetch(skillInfo.endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(args.input ?? args),
              })
              const skillResult = (await skillResponse.json()) as {
                success: boolean
                result?: unknown
                error?: string
                files?: Array<{ name: string; mimeType: string; base64: string }>
              }
              if (skillResult.success) {
                toolResult =
                  typeof skillResult.result === 'string'
                    ? skillResult.result
                    : JSON.stringify(skillResult.result ?? null, null, 2)

                // Tool returned attachments -> upload to MinIO + push message:files event
                if (skillResult.files && skillResult.files.length > 0) {
                  const filesWithKeys = await uploadFilesToMinio(
                    conversationId,
                    skillResult.files,
                    collectedFileMetadata
                  )
                  const filesEvent: ConversationEvent = {
                    type: 'message:files',
                    data: { files: filesWithKeys },
                  }
                  enqueue(encodeSSE(filesEvent))
                }
                if (taskId) {
                  db.insert(workLogs)
                    .values({
                      id: uuidv4(),
                      taskId,
                      employeeId,
                      logType: 'tool_call',
                      content: `Tool "${tc.function.name}" ${t('toolCallSuccess', 'en')}`,
                      metadata: makeLogMetadata(
                        { toolName: tc.function.name, skillId: skillInfo.skillId },
                        { i18nKey: 'toolCallSuccess', i18nParams: { name: tc.function.name } }
                      ),
                    })
                    .catch(() => {})
                }
              } else {
                toolResult = `${t('toolCallFailed', lang)}: ${skillResult.error ?? t('unknownError', lang)}`
                if (taskId) {
                  const failError = skillResult.error ?? t('unknownError', 'en')
                  db.insert(workLogs)
                    .values({
                      id: uuidv4(),
                      taskId,
                      employeeId,
                      logType: 'error',
                      content: `Tool "${tc.function.name}" ${t('toolCallFailedShort', 'en')}: ${failError}`,
                      metadata: makeLogMetadata(
                        { toolName: tc.function.name, skillId: skillInfo.skillId },
                        {
                          i18nKey: 'toolCallFailedShort',
                          i18nParams: { name: tc.function.name, error: failError },
                        }
                      ),
                    })
                    .catch(() => {})
                }
              }
            } catch (e) {
              const errMsg = e instanceof Error ? e.message : String(e)
              logger.error(`Skill tool call failed: ${skillInfo.skillId}`, { error: errMsg })
              toolResult = `${t('convToolCallFailed', lang)}: ${errMsg}`
              if (taskId) {
                db.insert(workLogs)
                  .values({
                    id: uuidv4(),
                    taskId,
                    employeeId,
                    logType: 'error',
                    content: t('toolCallError', 'en', { name: tc.function.name, error: errMsg }),
                    metadata: makeLogMetadata(
                      { toolName: tc.function.name, skillId: skillInfo.skillId },
                      {
                        i18nKey: 'toolCallError',
                        i18nParams: { name: tc.function.name, error: errMsg },
                      }
                    ),
                  })
                  .catch(() => {})
              }
            }
          }
        } else {
          toolResult = `${t('convUnknownTool', lang)}: ${tc.function.name}`
        }
      })

      // Save tool result message (intercepted responses not saved, avoid refreshing expiry window)
      if (!skipPersist) {
        const toolMsgId = uuidv4()
        await db.insert(conversationMessages).values({
          id: toolMsgId,
          conversationId,
          role: 'tool',
          content: toolResult,
          toolCallId: tc.id,
          toolName: tc.function.name,
        })
      }

      messages.push({
        role: 'tool',
        content: toolResult,
        tool_call_id: tc.id,
        name: tc.function.name,
      })

      const toolResultEvent: ConversationEvent = {
        type: 'tool:result',
        data: {
          toolCallId: tc.id,
          toolName: tc.function.name,
          result: toolResult.slice(0, 500),
          displayMessage: t('executedTool', lang, { name: toolDisplayName }),
        },
      }
      enqueue(encodeSSE(toolResultEvent))
    }

    previousRoundHadSopOutput = sopOutputThisRound

    // Continue loop to let LLM process tool results
  }

  // Max rounds reached
  logger.warn(`Conversation ${conversationId} reached max tool call rounds ${MAX_TOOL_ROUNDS}`)
  const errorEvent: ConversationEvent = {
    type: 'error',
    data: { message: t('toolRoundExceeded', lang) },
  }
  enqueue(encodeSSE(errorEvent))
  return { outputSummary: null, totalTokens }
}

interface LLMUsage {
  total: number
  input: number
  output: number
}

interface LLMResult {
  content: string | null
  toolCalls: ToolCall[] | null
  usage: LLMUsage
}

/** Fixed backoff delays (ms) between retry attempts — index = attempt number (0-based). */
const LLM_FETCH_RETRY_DELAYS_MS = [500, 1500]

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * POST to the LLM endpoint with retry, guarded to only run before any stream
 * bytes have reached the client. A transport-level failure (connection reset,
 * DNS blip) or a 429/5xx response is retried; 4xx responses are returned
 * as-is since retrying an auth/config error cannot succeed.
 */
async function fetchLLMResponse(
  url: string,
  body: Record<string, unknown>,
  config: ModelConfig
): Promise<Response> {
  const maxAttempts = LLM_FETCH_RETRY_DELAYS_MS.length + 1

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    let response: Response
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${config.apiKey}`,
        },
        body: JSON.stringify(body),
      })
    } catch (err) {
      if (attempt >= LLM_FETCH_RETRY_DELAYS_MS.length) throw err
      logger.warn(`LLM request network error, retrying (attempt ${attempt + 1}/${maxAttempts})`, {
        provider: config.providerId,
        model: config.model,
        error: err instanceof Error ? err.message : String(err),
      })
      await sleep(LLM_FETCH_RETRY_DELAYS_MS[attempt])
      continue
    }

    const isRetryableStatus = response.status === 429 || response.status >= 500
    if (response.ok || !isRetryableStatus || attempt >= LLM_FETCH_RETRY_DELAYS_MS.length) {
      return response
    }

    logger.warn(
      `LLM request returned retryable status, retrying (attempt ${attempt + 1}/${maxAttempts})`,
      {
        provider: config.providerId,
        model: config.model,
        status: response.status,
      }
    )
    await sleep(LLM_FETCH_RETRY_DELAYS_MS[attempt])
  }

  // Unreachable: the loop above always returns or throws on its last iteration.
  throw new Error('LLM request retry loop exited unexpectedly')
}

/**
 * Call OpenAI-compatible LLM API (streaming)
 */
async function callLLM(
  messages: EngineMessage[],
  tools: OpenAITool[],
  config: ModelConfig,
  emitDelta: (content: string) => void,
  onFirstToken: () => void
): Promise<LLMResult> {
  const body: Record<string, unknown> = {
    model: config.model,
    messages,
    stream: true,
    stream_options: { include_usage: true },
  }

  if (tools.length > 0) {
    body.tools = tools
    body.tool_choice = 'auto'
  }

  mergeExtraParams(body, config.extraParams)

  const response = await fetchLLMResponse(`${config.baseUrl}/chat/completions`, body, config)

  if (!response.ok) {
    const errorText = await response.text()
    throw new Error(`LLM API ${t('convError')} (${response.status}): ${errorText.slice(0, 300)}`)
  }

  if (!response.body) {
    throw new Error(t('emptyLLMResponse'))
  }

  return parseSSEStream(response.body, emitDelta, onFirstToken)
}

/**
 * Parse OpenAI-compatible SSE stream
 */
async function parseSSEStream(
  body: ReadableStream<Uint8Array>,
  emitDelta: (content: string) => void,
  onFirstToken: () => void
): Promise<LLMResult> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let contentAccum = ''
  let usage: LLMUsage = { total: 0, input: 0, output: 0 }
  const toolCallsAccum: Map<number, { id: string; name: string; arguments: string }> = new Map()

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''

      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed || !trimmed.startsWith('data: ')) continue
        const data = trimmed.slice(6)
        if (data === '[DONE]') continue

        let chunk: ChatCompletionChunk
        try {
          chunk = JSON.parse(data) as ChatCompletionChunk
        } catch {
          continue
        }

        if (chunk.usage) {
          usage = {
            total: chunk.usage.total_tokens ?? 0,
            input: chunk.usage.prompt_tokens ?? 0,
            output: chunk.usage.completion_tokens ?? 0,
          }
        }

        const delta = chunk.choices?.[0]?.delta
        if (!delta) continue

        if (delta.content || (delta.tool_calls && delta.tool_calls.length > 0)) {
          onFirstToken()
        }

        // Text content stream (buffer first, decide whether to push after stream ends)
        if (delta.content) {
          contentAccum += delta.content
        }

        // tool_calls streaming accumulation
        if (delta.tool_calls) {
          for (const tc of delta.tool_calls) {
            const existing = toolCallsAccum.get(tc.index)
            if (existing) {
              if (tc.function?.arguments) {
                existing.arguments += tc.function.arguments
              }
            } else {
              toolCallsAccum.set(tc.index, {
                id: tc.id ?? '',
                name: tc.function?.name ?? '',
                arguments: tc.function?.arguments ?? '',
              })
            }
          }
        }
      }
    }
  } finally {
    reader.releaseLock()
  }

  // Build tool_calls result
  let toolCalls: ToolCall[] | null = null
  if (toolCallsAccum.size > 0) {
    toolCalls = Array.from(toolCallsAccum.values()).map((tc) => ({
      id: tc.id,
      type: 'function' as const,
      function: {
        name: tc.name,
        arguments: tc.arguments,
      },
    }))
  }

  // Only push delta to frontend for plain text replies (no tool_calls)
  // With tool_calls, content is only for message history, not pushed, avoid generating duplicate content in later rounds
  if (!toolCalls && contentAccum) {
    emitDelta(contentAccum)
  }

  return {
    content: contentAccum || null,
    toolCalls,
    usage,
  }
}

/**
 * Upload tool-returned base64 files to MinIO, collect metadata
 *
 * @returns File list with key field (for SSE events to carry, frontend can build download links)
 */
async function uploadFilesToMinio(
  conversationId: string,
  files: Array<{ name: string; mimeType: string; base64: string }>,
  collectedFileMetadata: FileAttachment[]
): Promise<Array<{ name: string; mimeType: string; base64: string; key: string }>> {
  const filesWithKeys: Array<{ name: string; mimeType: string; base64: string; key: string }> = []

  for (const f of files) {
    try {
      const buf = Buffer.from(f.base64, 'base64')
      const attachment = await uploadConversationFile(conversationId, f.name, buf, f.mimeType)
      collectedFileMetadata.push(attachment)
      filesWithKeys.push({ ...f, key: attachment.key })
    } catch (err) {
      logger.warn('Tool returned file upload to MinIO failed', { fileName: f.name, error: err })
      // Keep base64 on upload failure, channel side can still send
      filesWithKeys.push({ ...f, key: '' })
    }
  }

  return filesWithKeys
}
