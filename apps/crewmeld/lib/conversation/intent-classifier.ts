/**
 * 意图分类器 —— 通过轻量级 LLM 调用决定用户意图路由
 *
 * 三种意图：
 * - knowledge_only:     纯知识库查询，无需执行 SOP/工作流
 * - knowledge_then_sop: 先查知识库获取上下文，再执行 SOP
 * - direct_sop:         直接执行 SOP/工作流，无需知识库
 */

import { createLogger } from '@crewmeld/logger'
import { t } from '@/lib/core/server-i18n'
import { mergeExtraParams } from './model-config'
import type { ConversationModelConfig, EngineMessage } from './types'

const logger = createLogger('IntentClassifier')

export type IntentType = 'knowledge_only' | 'knowledge_then_sop' | 'direct_sop'

export interface IntentResult {
  intent: IntentType
  reason: string
  /** 知识库检索关键词（仅 knowledge_only / knowledge_then_sop 使用） */
  searchQuery: string | null
}

/**
 * 构建意图分类的系统提示词 —— 告诉 LLM 可用资源和分类规则
 */
function buildClassifierPrompt(
  hasKnowledgeBase: boolean,
  hasTools: boolean,
  sopNames: string[],
  workflowNames: string[]
): string {
  const toolList = [
    ...sopNames.map((n) => `SOP: ${n}`),
    ...workflowNames.map((n) => `Workflow: ${n}`),
  ]

  return `You are an intent classifier. Based on the user's question, determine which processing path to follow.

## Available Resources
- Knowledge Base: ${hasKnowledgeBase ? 'Yes' : 'No'}
- Executable Tools: ${hasTools ? toolList.join(', ') : 'None'}

## Classification Rules

1. **knowledge_only** — The user is asking a question or querying information that can be answered solely by retrieving from the knowledge base, no action execution needed
   Examples: "What is the company's leave policy?", "What is the price for Product A?", "Where is the project report template?"

2. **knowledge_then_sop** — The user's request requires both knowledge base information as reference and SOP/workflow execution to complete the action
   Examples: "Apply for 3 days of leave based on company policy" (need to look up policy first, then execute leave SOP)
   Examples: "Send a quotation email to the client using the template" (need to look up template first, then execute email workflow)

3. **direct_sop** — The user explicitly requests an action to be performed, no additional knowledge base query needed
   Examples: "Check today's sales data", "Send a message to John", "Run data backup"

## Special Cases
- If there is no knowledge base, do not return knowledge_only or knowledge_then_sop
- If there are no executable tools, do not return direct_sop or knowledge_then_sop
- If the user is just chatting/greeting, return direct_sop (let the main conversation flow handle it)
- If neither is available (no knowledge base, no tools), return direct_sop

## Output Format
Return strict JSON only, no other content:
{"intent": "knowledge_only|knowledge_then_sop|direct_sop", "reason": "One sentence explaining the reason", "searchQuery": "Keywords for knowledge base retrieval (only for knowledge_only and knowledge_then_sop, otherwise null)"}`
}

/**
 * 调用 LLM 进行意图分类 —— 决定走知识库检索、知识库+SOP 还是直接执行 SOP
 */
export async function classifyIntent(
  userMessage: string,
  recentHistory: EngineMessage[],
  modelConfig: ConversationModelConfig,
  options: {
    hasKnowledgeBase: boolean
    hasTools: boolean
    sopNames: string[]
    workflowNames: string[]
  }
): Promise<IntentResult> {
  // 快速路径 1：无知识库 → 直接走 SOP（无需 LLM 调用，节省 token）
  if (!options.hasKnowledgeBase) {
    logger.info('No knowledge base bound, going directly to direct_sop')
    return { intent: 'direct_sop', reason: t('convIntentNoKnowledge'), searchQuery: null }
  }

  // 快速路径 2：无工具 → 直接走知识库检索（无需 LLM 调用，节省 token）
  if (!options.hasTools) {
    logger.info('No available tools, going directly to knowledge_only')
    return { intent: 'knowledge_only', reason: t('convIntentNoWorkflow'), searchQuery: userMessage }
  }

  // 既有知识库又有工具 → 需要 LLM 判断用户意图
  const systemPrompt = buildClassifierPrompt(
    options.hasKnowledgeBase,
    options.hasTools,
    options.sopNames,
    options.workflowNames
  )

  // 取最近 3 条消息作为上下文（足以判断意图，同时减少 token 消耗）
  const recentContext = recentHistory
    .filter((m) => m.role === 'user' || m.role === 'assistant')
    .slice(-3)
    .map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${(m.content ?? '').slice(0, 200)}`)
    .join('\n')

  const userPrompt = recentContext
    ? `Conversation context:\n${recentContext}\n\nCurrent user message: ${userMessage}`
    : `Current user message: ${userMessage}`

  // 构建分类请求消息
  const messages = [
    { role: 'system' as const, content: systemPrompt },
    { role: 'user' as const, content: userPrompt },
  ]

  try {
    // 调用 LLM（temperature=0 保证分类稳定，max_tokens=200 控制成本）
    const response = await fetch(`${modelConfig.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${modelConfig.apiKey}`,
      },
      body: JSON.stringify(
        mergeExtraParams(
          {
            model: modelConfig.model,
            messages,
            temperature: 0,
            max_tokens: 200,
            stream: false,
          },
          modelConfig.extraParams
        )
      ),
    })

    if (!response.ok) {
      const errorText = await response.text()
      logger.error('Intent classification LLM call failed', {
        status: response.status,
        error: errorText.slice(0, 200),
      })
      return fallbackClassify(options)
    }

    const data = (await response.json()) as {
      choices: Array<{ message: { content: string } }>
    }

    const content = data.choices?.[0]?.message?.content?.trim()
    if (!content) {
      logger.warn('Intent classification returned empty content')
      return fallbackClassify(options)
    }

    // 解析 LLM 返回的 JSON 分类结果
    const parsed = parseClassifierResponse(content)
    logger.info('Intent classification result', { intent: parsed.intent, reason: parsed.reason })
    return parsed
  } catch (error) {
    logger.error('Intent classification error', error)
    return fallbackClassify(options)
  }
}

/**
 * 解析分类器 JSON 响应 —— 容错提取 LLM 返回的 JSON（可能前后带有多余文本）
 */
function parseClassifierResponse(content: string): IntentResult {
  try {
    // 尝试提取 JSON（LLM 可能在 JSON 前后添加额外文本）
    const jsonMatch = content.match(/\{[\s\S]*\}/)
    if (!jsonMatch) {
      logger.warn('Cannot extract JSON from classifier response', {
        content: content.slice(0, 200),
      })
      return { intent: 'direct_sop', reason: t('convIntentParseFailed'), searchQuery: null }
    }

    const parsed = JSON.parse(jsonMatch[0]) as Record<string, unknown>
    const intent = parsed.intent as string
    const reason = (parsed.reason as string) ?? ''
    const searchQuery = (parsed.searchQuery as string) ?? null

    if (intent === 'knowledge_only' || intent === 'knowledge_then_sop' || intent === 'direct_sop') {
      return { intent, reason, searchQuery }
    }

    logger.warn('Unknown intent type', { intent })
    return {
      intent: 'direct_sop',
      reason: `${t('convIntentUnknown')}: ${intent}`,
      searchQuery: null,
    }
  } catch {
    logger.warn('Failed to parse classifier response JSON', { content: content.slice(0, 200) })
    return { intent: 'direct_sop', reason: t('convIntentParseError'), searchQuery: null }
  }
}

/**
 * 降级分类 —— LLM 调用失败时的兜底策略：优先走工具，其次知识库，最后默认 direct_sop
 */
function fallbackClassify(options: { hasKnowledgeBase: boolean; hasTools: boolean }): IntentResult {
  if (options.hasTools) {
    return { intent: 'direct_sop', reason: t('convIntentDegradeTools'), searchQuery: null }
  }
  if (options.hasKnowledgeBase) {
    return { intent: 'knowledge_only', reason: t('convIntentDegradeKnowledge'), searchQuery: null }
  }
  return { intent: 'direct_sop', reason: t('convIntentDegradeNoResource'), searchQuery: null }
}
