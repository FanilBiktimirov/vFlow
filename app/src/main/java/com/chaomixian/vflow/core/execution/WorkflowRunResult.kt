// 文件: main/java/com/chaomixian/vflow/core/execution/WorkflowRunResult.kt
package com.chaomixian.vflow.core.execution

import com.chaomixian.vflow.core.types.VObject

/**
 * 主工作流一次执行的最终结果。
 * 供需要等待执行结束的调用方（如 Remote API）通过 WorkflowExecutor.execute 的 completion 参数获取。
 */
data class WorkflowRunResult(
    val status: Status,
    /** 执行结束时的所有命名变量 */
    val namedVariables: Map<String, VObject>,
    val errorMessage: String? = null
) {
    enum class Status {
        /** 正常结束（包括"停止工作流"模块） */
        COMPLETED,
        /** 步骤失败、未捕获异常或因重入策略被拒绝 */
        FAILED,
        /** 被手动停止或被新的执行顶替 */
        CANCELLED,
        /** 超过工作流自身的最长执行时间 */
        TIMEOUT
    }
}
