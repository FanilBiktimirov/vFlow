package com.chaomixian.vflow.api.server

import com.chaomixian.vflow.api.model.*
import com.chaomixian.vflow.core.execution.WorkflowExecutor
import com.chaomixian.vflow.core.execution.WorkflowRunResult
import com.chaomixian.vflow.core.module.ExecutionResult
import com.chaomixian.vflow.core.types.VObject
import com.chaomixian.vflow.core.types.VObjectFactory
import com.chaomixian.vflow.core.types.basic.VBoolean
import com.chaomixian.vflow.core.types.basic.VDictionary
import com.chaomixian.vflow.core.types.basic.VList
import com.chaomixian.vflow.core.types.basic.VNull
import com.chaomixian.vflow.core.types.basic.VNumber
import com.chaomixian.vflow.core.types.basic.VString
import com.chaomixian.vflow.core.workflow.WorkflowManager
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/**
 * 执行管理器
 * 管理工作流执行的生命周期
 */
class ExecutionManager(
    private val workflowManager: WorkflowManager
) {

    companion object {
        /** 同步执行未指定 timeout 时，HTTP 请求最多等待的时间；工作流本身不会因此被停止 */
        private const val MAX_SYNC_WAIT_MS = 5 * 60 * 1000L
        /** 超时停止工作流后，等待其收尾并交出变量的时间 */
        private const val STOP_GRACE_MS = 5_000L
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val executions = ConcurrentHashMap<String, ExecutionRecord>()
    private val executionLogs = ConcurrentHashMap<String, MutableList<LogEntry>>()
    // 执行结束时的命名变量快照
    private val executionVariables = ConcurrentHashMap<String, Map<String, VObjectDto>>()
    // executionId -> WorkflowExecutor 的执行实例ID，用于停止具体的一次执行
    private val executorInstances = ConcurrentHashMap<String, String>()
    private val mutex = Mutex()

    /**
     * 执行工作流
     */
    fun executeWorkflow(
        workflowId: String,
        inputVariables: Map<String, VObjectDto>? = null,
        async: Boolean = true,
        timeout: Int? = null,
        callback: ((ExecutionResult) -> Unit)? = null
    ): ExecutionDetail {

        val workflow = workflowManager.getWorkflow(workflowId)
            ?: throw IllegalArgumentException("Workflow not found")

        // 注意：通过API执行时，允许执行禁用的工作流（用户手动触发）
        // 只有自动触发器（如定时器、事件）才需要检查isEnabled状态

        val executionId = "exec-${java.util.UUID.randomUUID()}"
        val now = System.currentTimeMillis()

        // 创建执行记录
        val record = ExecutionRecord(
            executionId = executionId,
            workflowId = workflowId,
            workflowName = workflow.name,
            status = ExecutionStatus.RUNNING,
            startedAt = now,
            completedAt = null,
            duration = null,
            triggeredBy = "manual",
            error = null
        )
        executions[executionId] = record
        executionLogs[executionId] = mutableListOf()

        // 无论同步还是异步，都在后台等待执行结束并收集结果
        val job = scope.launch {
            try {
                executeWorkflowInternal(workflow, executionId, inputVariables, timeout, callback)
            } catch (e: Exception) {
                handleExecutionError(executionId, e)
            }
        }

        if (!async) {
            // 同步执行：等待结果写入记录；设置了 timeout 时多留出停止收尾的时间
            val waitMs = timeout?.takeIf { it > 0 }?.let { it * 1000L + STOP_GRACE_MS + 1_000L }
                ?: MAX_SYNC_WAIT_MS
            runBlocking {
                withTimeoutOrNull(waitMs) { job.join() }
            }
        }

        return getExecutionDetail(executionId)!!
    }

    /**
     * 内部执行逻辑
     */
    private suspend fun executeWorkflowInternal(
        workflow: com.chaomixian.vflow.core.workflow.model.Workflow,
        executionId: String,
        inputVariables: Map<String, VObjectDto>?,
        timeout: Int?,
        callback: ((ExecutionResult) -> Unit)?
    ) {
        val context = workflowManager.context

        // 记录开始日志
        addLog(
            executionId,
            LogEntry(
                timestamp = System.currentTimeMillis(),
                level = LogLevel.INFO,
                stepIndex = null,
                moduleId = null,
                message = "Workflow execution started"
            )
        )

        try {
            // 执行工作流，输入变量作为命名变量预置
            val completion = CompletableDeferred<WorkflowRunResult>()
            val instanceId = WorkflowExecutor.execute(
                workflow = workflow,
                context = context,
                triggerStepId = workflow.manualTrigger()?.id,
                initialNamedVariables = inputVariables?.mapValues { (_, dto) -> dto.toVObject() },
                completion = completion
            )
            if (instanceId.isNotEmpty()) {
                executorInstances[executionId] = instanceId
                // 在登记实例之前就收到了停止请求
                if (executions[executionId]?.status == ExecutionStatus.CANCELLED) {
                    WorkflowExecutor.stopExecutionInstance(workflow.id, instanceId)
                }
            }

            val timeoutMs = timeout?.takeIf { it > 0 }?.let { it * 1000L }
            val result = if (timeoutMs == null) {
                completion.await()
            } else {
                withTimeoutOrNull(timeoutMs) { completion.await() }
            }

            if (result == null) {
                // 超时：停止这次执行，并尽量取回停止时的变量
                WorkflowExecutor.stopExecutionInstance(workflow.id, instanceId)
                val stopped = withTimeoutOrNull(STOP_GRACE_MS) { completion.await() }
                finishExecution(
                    executionId,
                    ExecutionStatus.TIMEOUT,
                    "Execution timed out after ${timeout}s",
                    stopped?.namedVariables
                )
                callback?.invoke(ExecutionResult.Failure("Execution timeout", "Execution timed out after ${timeout}s"))
            } else {
                finishExecution(executionId, result.status.toApiStatus(), result.errorMessage, result.namedVariables)
                callback?.invoke(
                    if (result.status == WorkflowRunResult.Status.COMPLETED) {
                        ExecutionResult.Success(result.namedVariables)
                    } else {
                        ExecutionResult.Failure("Execution ${result.status.name.lowercase()}", result.errorMessage ?: "")
                    }
                )
            }
        } catch (e: Exception) {
            handleExecutionError(executionId, e)
        } finally {
            executorInstances.remove(executionId)
        }
    }

    /**
     * 写入执行的最终状态和变量。
     * 若记录已不是 running（例如已通过 API 停止），只补充变量，不覆盖状态。
     */
    private fun finishExecution(
        executionId: String,
        status: ExecutionStatus,
        error: String?,
        variables: Map<String, VObject>?
    ) {
        val now = System.currentTimeMillis()
        if (variables != null && executions.containsKey(executionId)) {
            executionVariables[executionId] = variables.mapValues { (_, value) -> value.toDto() }
        }

        var finished = false
        executions.computeIfPresent(executionId) { _, record ->
            if (record.status != ExecutionStatus.RUNNING) {
                record
            } else {
                finished = true
                record.copy(
                    status = status,
                    completedAt = now,
                    duration = now - record.startedAt,
                    error = error
                )
            }
        }
        if (!finished) return

        addLog(
            executionId,
            LogEntry(
                timestamp = now,
                level = if (status == ExecutionStatus.COMPLETED) LogLevel.INFO else LogLevel.ERROR,
                stepIndex = null,
                moduleId = null,
                message = when (status) {
                    ExecutionStatus.COMPLETED -> "Workflow completed successfully"
                    else -> "Execution ${status.name.lowercase()}" + (error?.let { ": $it" } ?: "")
                }
            )
        )
    }

    /**
     * 处理执行错误
     */
    private fun handleExecutionError(executionId: String, error: Throwable) {
        val errorMessage = "${error.javaClass.simpleName}: ${error.message ?: "Unknown error"}"
        finishExecution(executionId, ExecutionStatus.FAILED, errorMessage, null)
    }

    private fun WorkflowRunResult.Status.toApiStatus(): ExecutionStatus = when (this) {
        WorkflowRunResult.Status.COMPLETED -> ExecutionStatus.COMPLETED
        WorkflowRunResult.Status.FAILED -> ExecutionStatus.FAILED
        WorkflowRunResult.Status.CANCELLED -> ExecutionStatus.CANCELLED
        WorkflowRunResult.Status.TIMEOUT -> ExecutionStatus.TIMEOUT
    }

    /**
     * API 输入变量 -> VObject。list / dictionary 由 Gson 解析为 List / Map，交给 VObjectFactory。
     */
    private fun VObjectDto.toVObject(): VObject {
        // Gson 不遵守 Kotlin 的非空约束，客户端漏传 type 时这里可能是 null
        val typeName: String? = type
        return when (typeName?.lowercase()) {
            VObjectDto.TYPE_NULL -> VNull
            VObjectDto.TYPE_STRING -> VString(value?.toString() ?: "")
            VObjectDto.TYPE_NUMBER -> (value as? Number ?: value?.toString()?.toDoubleOrNull())
                ?.let { VNumber(it) } ?: VNull
            VObjectDto.TYPE_BOOLEAN -> VBoolean(value as? Boolean ?: value?.toString().equals("true", ignoreCase = true))
            else -> VObjectFactory.from(value)
        }
    }

    /**
     * VObject -> API 输出 {type, value}。type 去掉 "vflow.type." 前缀，复杂类型的 value 为其文本表示。
     */
    private fun VObject.toDto(): VObjectDto {
        val typeName = type.id.removePrefix("vflow.type.")
        val value: Any? = when (this) {
            is VNull -> null
            is VString -> raw
            is VBoolean -> raw
            // Gson 默认拒绝序列化 NaN / Infinity
            is VNumber -> raw.toDouble().let { if (it.isNaN() || it.isInfinite()) asString() else raw }
            is VList -> raw.map { it.toDto() }
            is VDictionary -> raw.mapValues { (_, item) -> item.toDto() }
            else -> runCatching { asString() }.getOrElse { raw?.toString() }
        }
        return VObjectDto(typeName, value)
    }

    /**
     * 停止执行
     */
    fun stopExecution(executionId: String): Boolean {
        val record = executions[executionId] ?: return false

        if (record.status != ExecutionStatus.RUNNING) {
            return false
        }

        executorInstances[executionId]?.let { instanceId ->
            WorkflowExecutor.stopExecutionInstance(record.workflowId, instanceId)
        }

        val now = System.currentTimeMillis()
        executions[executionId] = record.copy(
            status = ExecutionStatus.CANCELLED,
            completedAt = now,
            duration = now - record.startedAt
        )

        addLog(
            executionId,
            LogEntry(
                timestamp = now,
                level = LogLevel.INFO,
                stepIndex = null,
                moduleId = null,
                message = "Execution cancelled by user"
            )
        )

        return true
    }

    /**
     * 获取执行详情
     */
    fun getExecutionDetail(executionId: String): ExecutionDetail? {
        val record = executions[executionId] ?: return null

        val errorResponse = if (record.error != null) {
            ErrorResponse(
                title = "Execution Error",
                message = record.error,
                stepIndex = null
            )
        } else null

        return ExecutionDetail(
            executionId = record.executionId,
            workflowId = record.workflowId,
            workflowName = record.workflowName,
            status = record.status,
            currentStepIndex = 0, // TODO: 跟踪当前步骤
            totalSteps = 0, // TODO: 获取工作流步骤数
            currentStep = null,
            startedAt = record.startedAt,
            completedAt = record.completedAt,
            duration = record.duration,
            outputs = emptyMap(), // TODO: 收集输出
            error = errorResponse,
            variables = executionVariables[executionId] ?: emptyMap()
        )
    }

    /**
     * 获取执行日志
     */
    fun getExecutionLogs(
        executionId: String,
        level: LogLevel? = null,
        stepIndex: Int? = null,
        limit: Int = 100,
        offset: Int = 0
    ): ExecutionLogsResponse {
        val allLogs = executionLogs[executionId] ?: emptyList()

        var filteredLogs = allLogs

        // 应用过滤器
        if (level != null) {
            filteredLogs = filteredLogs.filter { it.level == level }
        }
        if (stepIndex != null) {
            filteredLogs = filteredLogs.filter { it.stepIndex == stepIndex }
        }

        val total = filteredLogs.size
        val paginatedLogs = filteredLogs
            .drop(offset)
            .take(limit)

        return ExecutionLogsResponse(
            logs = paginatedLogs,
            total = total,
            limit = limit,
            offset = offset
        )
    }

    /**
     * 列出执行记录
     */
    fun listExecutions(
        workflowId: String? = null,
        status: ExecutionStatus? = null,
        limit: Int = 20,
        offset: Int = 0
    ): ExecutionListResponse {
        var filtered = executions.values.toList()

        // 应用过滤器
        if (workflowId != null) {
            filtered = filtered.filter { it.workflowId == workflowId }
        }
        if (status != null) {
            filtered = filtered.filter { it.status == status }
        }

        // 按时间倒序排序
        filtered = filtered.sortedByDescending { it.startedAt }

        val total = filtered.size
        val paginated = filtered.drop(offset).take(limit)

        return ExecutionListResponse(
            executions = paginated,
            total = total,
            limit = limit,
            offset = offset
        )
    }

    /**
     * 删除执行历史
     */
    fun deleteExecutionHistory(
        executionId: String? = null,
        workflowId: String? = null,
        olderThan: Long? = null
    ): Int {
        var toDelete = setOf<String>()

        when {
            executionId != null -> toDelete = setOf(executionId)
            workflowId != null -> {
                toDelete = executions
                    .filter { it.value.workflowId == workflowId }
                    .keys
            }
            olderThan != null -> {
                toDelete = executions
                    .filter { it.value.startedAt < olderThan }
                    .keys
            }
        }

        toDelete.forEach {
            executions.remove(it)
            executionLogs.remove(it)
            executionVariables.remove(it)
        }

        return toDelete.size
    }

    /**
     * 添加日志
     */
    private fun addLog(executionId: String, log: LogEntry) {
        executionLogs.getOrPut(executionId) { mutableListOf() }.add(log)
    }

    /**
     * 清理旧的执行记录
     */
    suspend fun cleanupOldExecutions(maxAge: Long = 86400000L * 7) { // 7天
        val now = System.currentTimeMillis()
        val oldExecutions = executions.filter { now - it.value.startedAt > maxAge }.keys

        mutex.withLock {
            oldExecutions.forEach {
                executions.remove(it)
                executionLogs.remove(it)
                executionVariables.remove(it)
            }
        }
    }

    /**
     * 获取统计信息
     */
    fun getStats(): ExecutionStats {
        val allExecutions = executions.values
        val now = System.currentTimeMillis()
        val todayStart = now - (now % 86400000L)

        return ExecutionStats(
            totalExecutions = allExecutions.size,
            runningExecutions = allExecutions.count { it.status == ExecutionStatus.RUNNING },
            todayExecutions = allExecutions.count { it.startedAt >= todayStart },
            successfulExecutions = allExecutions.count { it.status == ExecutionStatus.COMPLETED },
            failedExecutions = allExecutions.count { it.status == ExecutionStatus.FAILED }
        )
    }
}

/**
 * 执行统计
 */
data class ExecutionStats(
    val totalExecutions: Int,
    val runningExecutions: Int,
    val todayExecutions: Int,
    val successfulExecutions: Int,
    val failedExecutions: Int
)
