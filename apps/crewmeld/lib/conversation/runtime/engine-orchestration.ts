import {
  createRuntimeCompletionLogger,
  type RuntimeDurationName,
  type RuntimeTiming,
  type RuntimeTimingSnapshot,
} from './timing'

export function createConversationRuntimeBoundary(
  timing: RuntimeTiming,
  onComplete: (snapshot: RuntimeTimingSnapshot) => void
): { complete: () => void; load: <T>(operation: () => Promise<T>) => Promise<T> } {
  const complete = createRuntimeCompletionLogger(timing, onComplete)
  return {
    complete,
    async load<T>(operation: () => Promise<T>): Promise<T> {
      try {
        return await operation()
      } catch (error) {
        complete()
        throw error
      }
    },
  }
}

/**
 * 包装异步操作并自动计时 —— 在操作前后调用 timing.mark/measure，无论成功或异常都会记录耗时。
 * 用于 engine.ts 中意图分类（routeMs）、知识检索（retrievalMs）、身份解析（identityMs）等阶段的耗时追踪。
 */
export async function measureRuntimeOperation<T>(
  timing: RuntimeTiming,
  name: RuntimeDurationName,
  operation: () => Promise<T>
): Promise<T> {
  const start = timing.mark(name) // 打起始时间戳
  try {
    return await operation()
  } finally {
    timing.measure(name, start) // 计算耗时并记录到 timing 快照
  }
}

interface ConversationRuntimeOrchestration {
  timing: RuntimeTiming
  execute: (timing: RuntimeTiming) => Promise<void>
  onError: (error: unknown) => Promise<void>
  onComplete: (snapshot: RuntimeTimingSnapshot) => void
}

export async function runConversationRuntime({
  timing,
  execute,
  onError,
  onComplete,
}: ConversationRuntimeOrchestration): Promise<void> {
  try {
    await execute(timing)
  } catch (error) {
    await onError(error)
  } finally {
    onComplete(timing.snapshot())
  }
}

interface ConversationStreamStart extends ConversationRuntimeOrchestration {
  initialize: () => Promise<void>
}

export async function runConversationStreamStart({
  initialize,
  ...runtime
}: ConversationStreamStart): Promise<void> {
  await runConversationRuntime({
    ...runtime,
    execute: async (timing) => {
      await initialize()
      await runtime.execute(timing)
    },
  })
}
