// 文件: main/java/com/chaomixian/vflow/core/execution/WorkflowExecutor.kt
package com.chaomixian.vflow.core.execution

import android.content.Context
import android.os.Parcelable
import com.chaomixian.vflow.R
import com.chaomixian.vflow.core.locale.LocaleManager
import com.chaomixian.vflow.core.logging.LogEntry
import com.chaomixian.vflow.core.logging.LogManager
import com.chaomixian.vflow.core.logging.LogMessageKey
import com.chaomixian.vflow.core.logging.LogStatus
import com.chaomixian.vflow.core.module.*
import com.chaomixian.vflow.core.types.VObject
import com.chaomixian.vflow.core.types.VTypeRegistry
import com.chaomixian.vflow.core.types.VObjectFactory
import com.chaomixian.vflow.core.types.basic.VNull
import com.chaomixian.vflow.core.types.basic.VString
import com.chaomixian.vflow.core.types.basic.VNumber
import com.chaomixian.vflow.core.types.basic.VBoolean
import com.chaomixian.vflow.core.types.basic.VList
import com.chaomixian.vflow.core.types.basic.VDictionary
import com.chaomixian.vflow.core.types.complex.VImage
import com.chaomixian.vflow.core.utils.StorageManager
import com.chaomixian.vflow.core.workflow.model.ActionStep
import com.chaomixian.vflow.core.workflow.model.ActionStepExecutionSettings
import com.chaomixian.vflow.core.workflow.model.Workflow
import com.chaomixian.vflow.core.workflow.model.WorkflowReentryBehavior
import com.chaomixian.vflow.core.workflow.module.logic.*
import com.chaomixian.vflow.extension.ExternalModuleManager
import com.chaomixian.vflow.services.ExecutionNotificationManager
import com.chaomixian.vflow.services.ExecutionNotificationState
import com.chaomixian.vflow.services.ExecutionUIService
import com.chaomixian.vflow.services.ServiceStateBus
import com.chaomixian.vflow.services.WorkflowExecutionWakeLockManager
import kotlinx.coroutines.*
import kotlinx.coroutines.TimeoutCancellationException
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.coroutineContext
import com.chaomixian.vflow.core.logging.DebugLogger as GlobalDebugLogger

object WorkflowExecutor {

    private data class ActiveExecution(
        val instanceId: String,
        val job: Job
    )

    private val executorScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    // 使用 ConcurrentHashMap 来安全地跟踪正在运行的工作流及其对应的 Job
    private val runningWorkflows = ConcurrentHashMap<String, MutableList<ActiveExecution>>()
    // 用于标记工作流是否被 Stop 信号正常终止
    private val stoppedWorkflows = ConcurrentHashMap<String, Boolean>()
    // 用于标记某次执行是否已通过失败分支完成收尾（值为错误信息），避免 finally 再次广播结束态
    private val failedExecutions = ConcurrentHashMap<String, String>()

    // 用于存储每个正在运行的工作流的详细日志
    private val executionLogs = ConcurrentHashMap<String, StringBuilder>()

    // 用于在协程间传递当前 Root Workflow ID 的 ThreadLocal
    private val currentRootWorkflowId = ThreadLocal<String>()

    /**
     * 本地 DebugLogger 代理对象。
     * 它拦截所有的日志调用，转发给全局 Logger，同时记录到当前工作流的 executionLogs 中。
     */
    private object DebugLogger {
        private val dateFormat = SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault())

        fun d(tag: String, message: String, throwable: Throwable? = null) {
            GlobalDebugLogger.d(tag, message, throwable)
            appendToLog("D", tag, message, throwable)
        }

        fun i(tag: String, message: String, throwable: Throwable? = null) {
            GlobalDebugLogger.i(tag, message, throwable)
            appendToLog("I", tag, message, throwable)
        }

        fun w(tag: String, message: String, throwable: Throwable? = null) {
            GlobalDebugLogger.w(tag, message, throwable)
            appendToLog("W", tag, message, throwable)
        }

        fun e(tag: String, message: String, throwable: Throwable? = null) {
            GlobalDebugLogger.e(tag, message, throwable)
            appendToLog("E", tag, message, throwable)
        }

        private fun appendToLog(level: String, tag: String, message: String, throwable: Throwable?) {
            // 从 ThreadLocal 中获取当前上下文的工作流 ID
            val workflowId = currentRootWorkflowId.get() ?: return
            val sb = executionLogs[workflowId] ?: return

            val time = dateFormat.format(Date())
            sb.append("[$time] $level/$tag: $message\n")
            if (throwable != null) {
                sb.append(throwable.stackTraceToString()).append("\n")
            }
        }
    }

    /**
     * 检查一个工作流当前是否正在运行。
     * @param workflowId 要检查的工作流的ID。
     * @return 如果正在运行，则返回 true。
     */
    fun isRunning(workflowId: String): Boolean {
        return runningWorkflows[workflowId]?.isNotEmpty() == true
    }

    /**
     * 停止一个正在执行的工作流。
     * @param workflowId 要停止的工作流的ID。
     */
    fun stopExecution(workflowId: String) {
        runningWorkflows[workflowId]?.toList()?.forEach {
            it.job.cancel() // 取消 Coroutine Job
            // 这里无法直接使用本地 DebugLogger 记录到日志，因为不在协程上下文中
            GlobalDebugLogger.d("WorkflowExecutor", "工作流 '$workflowId' 已被用户手动停止。")
        }
    }

    /**
     * 停止某一次具体的执行，不影响同一工作流的其他并行执行。
     * @param workflowId 工作流ID。
     * @param executionInstanceId execute 返回的执行实例ID。
     */
    fun stopExecutionInstance(workflowId: String, executionInstanceId: String) {
        runningWorkflows[workflowId]?.toList()
            ?.firstOrNull { it.instanceId == executionInstanceId }
            ?.job?.cancel()
    }

    /**
     * 执行一个工作流。这是最外层的入口点。
     * @param workflow 要执行的工作流。
     * @param context Android 上下文。
     * @param triggerData (可选) 触发器传入的外部数据。
     * @param initialNamedVariables (可选) 预置的命名变量，执行开始前放入 namedVariables。
     * @param completion (可选) 执行结束时以最终状态和命名变量完成，用于等待执行结束。
     * @return 执行实例ID；因重入策略被拒绝时返回空字符串。
     */
    fun execute(
        workflow: Workflow,
        context: Context,
        triggerData: Parcelable? = null,
        triggerStepId: String? = null,
        initialNamedVariables: Map<String, VObject>? = null,
        completion: CompletableDeferred<WorkflowRunResult>? = null
    ): String {
        when (workflow.reentryBehavior) {
            WorkflowReentryBehavior.BLOCK_NEW -> {
                if (isRunning(workflow.id)) {
                    GlobalDebugLogger.w("WorkflowExecutor", "工作流 '${workflow.name}' 已在运行，忽略新的执行请求。")
                    addReentryLog(workflow, LogMessageKey.REENTRY_BLOCKED_NEW_EXECUTION)
                    completion?.complete(
                        WorkflowRunResult(
                            status = WorkflowRunResult.Status.FAILED,
                            namedVariables = initialNamedVariables.orEmpty(),
                            errorMessage = "Workflow is already running, new execution was blocked"
                        )
                    )
                    return ""
                }
            }
            WorkflowReentryBehavior.STOP_CURRENT_AND_RUN_NEW -> {
                if (isRunning(workflow.id)) {
                    GlobalDebugLogger.i("WorkflowExecutor", "工作流 '${workflow.name}' 再次触发，停止当前执行并启动新执行。")
                    addReentryLog(workflow, LogMessageKey.REENTRY_STOPPED_RUNNING_EXECUTION)
                    stopExecution(workflow.id)
                }
            }
            WorkflowReentryBehavior.ALLOW_PARALLEL -> {
                if (isRunning(workflow.id)) {
                    GlobalDebugLogger.i("WorkflowExecutor", "工作流 '${workflow.name}' 再次触发，允许并行执行。")
                }
            }
        }
        if (!isRunning(workflow.id)) {
            stoppedWorkflows.remove(workflow.id)
            executionLogs.remove(workflow.id)
        }

        val logBuffer = executionLogs.getOrPut(workflow.id) { StringBuilder() }
        synchronized(logBuffer) {
            if (logBuffer.isNotEmpty()) {
                logBuffer.append("\n")
            }
            logBuffer.append("--- 开始执行: ${workflow.name} ---\n")
            logBuffer.append("ID: ${workflow.id}\n")
            if (triggerData != null) logBuffer.append("触发数据: $triggerData\n")
        }

        val executionInstanceId = "${workflow.id}_${UUID.randomUUID()}"
        // 命名变量在 launch 之外创建，以便结束时（包括异常兜底）都能取到最终值
        val namedVariables = ConcurrentHashMap<String, VObject>().apply {
            initialNamedVariables?.let { putAll(it) }
        }
        val job = executorScope.launch {
            // 将 workflow.id 注入到当前协程及其子协程的上下文中
            // 这样，在此作用域内调用的所有本地 DebugLogger 方法都能通过 ThreadLocal 获取到 ID
            withContext(currentRootWorkflowId.asContextElement(value = workflow.id)) {

                // 广播开始执行的状态，初始索引为-1表示准备阶段
                ExecutionStateBus.postState(
                    ExecutionState.Running(
                        workflowId = workflow.id,
                        executionInstanceId = executionInstanceId,
                        stepIndex = -1
                    )
                )
                DebugLogger.d("WorkflowExecutor", "开始执行主工作流: ${workflow.name} (ID: ${workflow.id})")
                ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Running(0, "正在开始..."))
                WorkflowExecutionWakeLockManager.acquireIfEnabled(context.applicationContext, executionInstanceId)

                // 创建本次执行的独立工作目录
                val workDir = File(StorageManager.tempDir, "exec_$executionInstanceId")
                if (!workDir.exists()) workDir.mkdirs()

                var isTimeout = false
                var uncaughtError: Exception? = null

                try {
                    // 创建并注册执行期间所需的服务
                    val services = ExecutionServices()
                    ServiceStateBus.getAccessibilityService()?.let { services.add(it) }
                    services.add(ExecutionUIService(context.applicationContext))

                    // 创建初始执行上下文（所有变量都统一为 VObject 类型）
                    val initialContext = ExecutionContext(
                        applicationContext = context.applicationContext,
                        variables = mutableMapOf<String, VObject>(),
                        magicVariables = mutableMapOf<String, VObject>(),
                        services = services,
                        allSteps = workflow.steps,
                        currentStepIndex = -1, // 初始索引
                        stepOutputs = mutableMapOf(),
                        loopStack = Stack(),
                        triggerData = triggerData,
                        namedVariables = namedVariables,
                        workflowStack = Stack<String>().apply { push(workflow.id) },
                        workDir = workDir
                    )

                    // 超时限制
                    val maxExecutionTime = workflow.maxExecutionTime

                    if (maxExecutionTime != null && maxExecutionTime > 0) {
                        try {
                            withTimeout(maxExecutionTime * 1000L) {
                                seedTriggerOutputs(workflow, initialContext, triggerStepId)
                                executeWorkflowInternal(workflow, initialContext, executionInstanceId)
                            }
                        } catch (e: TimeoutCancellationException) {
                            DebugLogger.e("WorkflowExecutor", "工作流执行超时（最大 ${maxExecutionTime} 秒）")
                            ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Cancelled("执行超时（${maxExecutionTime}秒）"))
                            isTimeout = true

                            // 在主线程显示 Toast 提示
                            try {
                                withContext(Dispatchers.Main) {
                                    android.widget.Toast.makeText(
                                        initialContext.applicationContext,
                                        "工作流执行超时（${maxExecutionTime}秒）",
                                        android.widget.Toast.LENGTH_LONG
                                    ).show()
                                }
                            } catch (toastException: Exception) {
                                DebugLogger.w("WorkflowExecutor", "显示超时 Toast 失败", toastException)
                            }
                        }
                    } else {
                        seedTriggerOutputs(workflow, initialContext, triggerStepId)
                        executeWorkflowInternal(workflow, initialContext, executionInstanceId)
                    }

                    if (!isTimeout) {
                        ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Completed("执行完毕"))
                    }

                } catch (e: CancellationException) {
                    if (!isTimeout) {
                        DebugLogger.d("WorkflowExecutor", "主工作流 '${workflow.name}' 已被取消。")
                        ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Cancelled("已停止"))
                    }
                } catch (e: Exception) {
                    uncaughtError = e
                    DebugLogger.e("WorkflowExecutor", "主工作流 '${workflow.name}' 执行时发生未捕获的异常。", e)
                    ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Cancelled("执行异常"))
                } finally {
                    // 捕获当前协程的取消状态，因为进入 withContext(NonCancellable) 后 isActive 将总是 true
                    val isCoroutineCancelled = !isActive

                    // 使用 NonCancellable 上下文，确保即使协程被取消（如手动停止），清理和广播逻辑也能完整执行
                    withContext(NonCancellable) {
                        WorkflowExecutionWakeLockManager.release(executionInstanceId)

                        // 执行结束后清理工作目录
                        try {
                            if (workDir.exists()) {
                                workDir.deleteRecursively()
                                DebugLogger.d("WorkflowExecutor", "已清理工作目录")
                            }
                        } catch (e: Exception) {
                            DebugLogger.w("WorkflowExecutor", "清理工作目录失败: ${e.message}")
                        }

                        // 广播最终状态
                        val wasStopped = stoppedWorkflows[executionInstanceId] == true
                        val failureMessage = failedExecutions.remove(executionInstanceId)
                        val wasFailureHandled = failureMessage != null
                        // 如果协程不再活跃且不是因为“停止工作流”模块导致的，则视为手动取消
                        val wasCancelled = isCoroutineCancelled && !wasStopped

                        val fullLog = executionLogs[workflow.id]?.toString() ?: ""
                        val hasActiveExecutions = unregisterExecution(workflow.id, executionInstanceId)
                        stoppedWorkflows.remove(executionInstanceId)

                        if (!hasActiveExecutions) {
                            executionLogs.remove(workflow.id)
                        }

                        if (!wasFailureHandled) {
                            if (isTimeout) {
                                // 超时取消，广播 Cancelled 状态
                                ExecutionStateBus.postState(
                                    ExecutionState.Cancelled(
                                        workflowId = workflow.id,
                                        executionInstanceId = executionInstanceId,
                                        detailedLog = fullLog
                                    )
                                )
                            } else if (wasCancelled) {
                                ExecutionStateBus.postState(
                                    ExecutionState.Cancelled(
                                        workflowId = workflow.id,
                                        executionInstanceId = executionInstanceId,
                                        detailedLog = fullLog
                                    )
                                )
                            } else {
                                // 正常结束或Stop信号都视为Finished
                                ExecutionStateBus.postState(
                                    ExecutionState.Finished(
                                        workflowId = workflow.id,
                                        executionInstanceId = executionInstanceId,
                                        detailedLog = fullLog
                                    )
                                )
                            }
                            DebugLogger.d("WorkflowExecutor", "主工作流 '${workflow.name}' 执行完毕。")
                        }

                        // 在通知延迟之前交出结果，等待方不必多等 3 秒
                        val error = uncaughtError
                        completion?.complete(
                            when {
                                wasFailureHandled -> WorkflowRunResult(
                                    WorkflowRunResult.Status.FAILED, HashMap(namedVariables), failureMessage
                                )
                                error != null -> WorkflowRunResult(
                                    WorkflowRunResult.Status.FAILED,
                                    HashMap(namedVariables),
                                    formatExecutionExceptionMessage(error, "执行异常")
                                )
                                isTimeout -> WorkflowRunResult(
                                    WorkflowRunResult.Status.TIMEOUT,
                                    HashMap(namedVariables),
                                    "Workflow exceeded its max execution time (${workflow.maxExecutionTime}s)"
                                )
                                wasCancelled -> WorkflowRunResult(WorkflowRunResult.Status.CANCELLED, HashMap(namedVariables))
                                else -> WorkflowRunResult(WorkflowRunResult.Status.COMPLETED, HashMap(namedVariables))
                            }
                        )
                        // 延迟后取消通知，给用户时间查看最终状态
                        delay(3000)
                        ExecutionNotificationManager.cancelNotification()
                    }
                }
            }
        }
        registerExecution(workflow.id, executionInstanceId, job)
        // 兜底：若协程在进入 try 之前就结束（如启动前被取消），保证等待方不会一直挂起
        completion?.let { deferred ->
            job.invokeOnCompletion { cause ->
                deferred.complete(
                    WorkflowRunResult(
                        status = when (cause) {
                            null -> WorkflowRunResult.Status.COMPLETED
                            is CancellationException -> WorkflowRunResult.Status.CANCELLED
                            else -> WorkflowRunResult.Status.FAILED
                        },
                        namedVariables = HashMap(namedVariables),
                        errorMessage = cause?.takeUnless { it is CancellationException }
                            ?.let { formatExecutionExceptionMessage(it, "执行异常") }
                    )
                )
            }
        }
        return executionInstanceId
    }

    /**
     * 执行一个子工作流并返回结果。
     * @param workflow 要执行的子工作流。
     * @param parentContext 父工作流的执行上下文。
     * @return 包含子工作流返回值和所有变量的结果。
     */
    suspend fun executeSubWorkflow(workflow: Workflow, parentContext: ExecutionContext): SubWorkflowResult {
        DebugLogger.d("WorkflowExecutor", "开始执行子工作流: ${workflow.name}")

        // 创建子工作流的上下文，继承大部分父上下文状态，但使用自己的步骤列表和调用栈
        val subWorkflowContext = parentContext.copy(
            allSteps = workflow.steps,
            workflowStack = Stack<String>().apply {
                addAll(parentContext.workflowStack)
                push(workflow.id)
            }
            // 子工作流共享父工作流的 namedVariables，这样命名变量会自动传递
        )

        seedTriggerOutputs(workflow, subWorkflowContext, workflow.manualTrigger()?.id)

        // 调用内部执行循环，获取返回值
        val returnValue = executeWorkflowInternal(workflow, subWorkflowContext, workflow.id)

        // 返回值 + 命名变量
        return SubWorkflowResult(
            returnValue = returnValue,
            namedVariables = subWorkflowContext.namedVariables.toMap()
        )
    }

    private suspend fun seedTriggerOutputs(
        workflow: Workflow,
        executionContext: ExecutionContext,
        triggerStepId: String?
    ) {
        val effectiveTriggerId = triggerStepId ?: return
        val triggerStep = workflow.getTrigger(effectiveTriggerId) ?: return
        val module = ModuleRegistry.getModule(triggerStep.moduleId) ?: return

        val triggerContext = executionContext.copy(
            variables = triggerStep.parameters.mapValues { (_, value) -> VObjectFactory.from(value) }.toMutableMap(),
            magicVariables = mutableMapOf(),
            currentStepIndex = -1
        )

        when (val result = module.execute(triggerContext) {}) {
            is ExecutionResult.Success -> {
                val triggerOutputs = VObjectFactory.fromMapAny(result.outputs)
                val triggerOutputSchema = outputSchema(module, triggerStep, workflow.allSteps)
                workflow.triggers
                    .filter { sameTypeTrigger ->
                        sameTypeTrigger.moduleId == triggerStep.moduleId &&
                            outputSchema(module, sameTypeTrigger, workflow.allSteps) == triggerOutputSchema
                    }
                    .forEach { sameTypeTrigger ->
                        executionContext.stepOutputs[sameTypeTrigger.id] = triggerOutputs
                    }
            }
            is ExecutionResult.Failure -> {
                throw IllegalStateException(result.errorMessage)
            }
            else -> Unit
        }
    }

    private fun outputSchema(
        module: ActionModule,
        step: ActionStep,
        allSteps: List<ActionStep>
    ): List<Triple<String, String, String?>> {
        return module.getDynamicOutputs(step, allSteps).map { output ->
            Triple(output.id, output.typeName, output.listElementType)
        }
    }

    /**
     * 核心的工作流执行循环。
     * @param workflow 要执行的工作流。
     * @param initialContext 初始执行上下文。
     * @return 子工作流的返回值，对于主工作流总是返回 null。
     */
    private suspend fun executeWorkflowInternal(
        workflow: Workflow,
        initialContext: ExecutionContext,
        executionInstanceId: String
    ): Any? {
        val stepOutputs = initialContext.stepOutputs.toMutableMap()
        val namedVariables = initialContext.namedVariables
        val loopStack = initialContext.loopStack
        var pc = 0 // 程序计数器
        var returnValue: Any? = null // 用于存储子工作流的返回值

        while (pc < workflow.steps.size && coroutineContext.isActive) {
            val step = workflow.steps[pc]
            var module = ModuleRegistry.getModule(step.moduleId)
            if (module == null) {
                ExternalModuleManager.loadModules(initialContext.applicationContext, force = true)
                module = ModuleRegistry.getModule(step.moduleId)
            }
            if (module == null) {
                DebugLogger.w("WorkflowExecutor", "模块未找到: ${step.moduleId}")
                pc++
                continue
            }

            if (step.isDisabled) {
                val behavior = module.blockBehavior
                val nextPc = when (behavior.type) {
                    BlockType.BLOCK_START -> {
                        val endPos = BlockNavigator.findEndBlockPosition(workflow.steps, pc, behavior.pairingId)
                        if (endPos != -1) endPos + 1 else pc + 1
                    }
                    BlockType.BLOCK_MIDDLE -> {
                        val endPos = BlockNavigator.findEndBlockPosition(workflow.steps, pc, behavior.pairingId)
                        if (endPos != -1) endPos + 1 else pc + 1
                    }
                    else -> pc + 1
                }
                DebugLogger.d("WorkflowExecutor", "[${workflow.name}][#${pc + 1}] -> 跳过已禁用步骤: ${module.metadata.name}")
                pc = nextPc
                continue
            }

            // 如果在循环内，注入循环变量
            if (loopStack.isNotEmpty()) {
                val loopState = loopStack.peek()
                val loopStartPos = BlockNavigator.findCurrentLoopStartPosition(workflow.steps, pc)
                if (loopStartPos != -1) {
                    val loopStartStep = workflow.steps[loopStartPos]

                    val loopOutputs = when {
                        loopStartStep.moduleId == LOOP_START_ID && loopState is LoopState.CountLoopState -> {
                            mapOf(
                                "loop_index" to VNumber(loopState.currentIteration + 1),
                                "loop_total" to VNumber(loopState.totalIterations)
                            )
                        }
                        loopStartStep.moduleId == FOREACH_START_ID && loopState is LoopState.ForEachLoopState -> {
                            mapOf(
                                "index" to VNumber(loopState.currentIndex + 1),
                                "item" to loopState.itemList.getOrNull(loopState.currentIndex)
                            )
                        }
                        else -> null
                    }

                    if (loopOutputs != null) {
                        stepOutputs[loopStartStep.id] = VObjectFactory.fromMapAny(loopOutputs)
                    }
                }
            }

            // 广播当前执行步骤的索引
            ExecutionStateBus.postState(
                ExecutionState.Running(
                    workflowId = workflow.id,
                    executionInstanceId = executionInstanceId,
                    stepIndex = pc
                )
            )

            // 更新进度通知
            val progress = (pc * 100) / workflow.steps.size
            val progressMessage = "步骤 ${pc + 1}/${workflow.steps.size}: ${module.metadata.name}"
            ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Running(progress, progressMessage))

            // 为当前步骤创建执行上下文
            // 注意：step.parameters 是 Map<String, Any?>，需要转换为 Map<String, VObject>
            val executionContext = initialContext.copy(
                variables = step.parameters.mapValues { (_, value) -> VObjectFactory.from(value) }.toMutableMap(),
                magicVariables = mutableMapOf<String, VObject>(),
                currentStepIndex = pc,
                stepOutputs = stepOutputs,
                loopStack = loopStack,
                namedVariables = namedVariables
            )

            val parameterResolutionFailure = runCatching {
                // 统一解析所有变量引用
                step.parameters.forEach { (key, value) ->
                    if (value is String) {
                        when {
                            // 1. 魔法变量 ({{...}})
                            value.isMagicVariable() -> {
                                VariableResolver.resolveSingleVariableReference(value, executionContext)?.let { resolved ->
                                    executionContext.magicVariables[key] = resolved
                                }
                            }

                            // 2. 命名变量 ([[...]])
                            value.isNamedVariable() -> {
                                VariableResolver.resolveSingleVariableReference(value, executionContext)?.let { resolved ->
                                    executionContext.magicVariables[key] = resolved
                                }
                            }

                            // 3. 混合情况：包含变量引用的普通字符串 (如 "静态文本{{uuid.success}}")
                            VariableResolver.isComplex(value) -> {
                                // 使用 VariableResolver 解析混合字符串，然后包装为 VString
                                val resolved = VariableResolver.resolve(value, executionContext)
                                executionContext.magicVariables[key] = VString(resolved)
                            }
                        }
                    }
                }
            }.exceptionOrNull()

            val preExecutionFailure = parameterResolutionFailure?.let { error ->
                val message = formatExecutionExceptionMessage(error, "参数解析失败")
                DebugLogger.e("WorkflowExecutor", "步骤参数解析失败: $message")
                ExecutionResult.Failure("参数解析失败", message)
            }

            // 使用本地 DebugLogger，它会自动记录到 detailedLog
            DebugLogger.d("WorkflowExecutor", "[${workflow.name}][#${pc + 1}] -> 执行: ${module.metadata.name}")

            // --- 错误处理与重试逻辑 ---
            val executionSettings = ActionStepExecutionSettings.fromParameters(step.parameters)
            val errorPolicy = executionSettings.policy
            val retryCount = executionSettings.retryCount
            val retryInterval = executionSettings.retryIntervalMillis

            var attempt = 0
            var finalResult: ExecutionResult? = null

            while (attempt <= retryCount || errorPolicy != ActionStepExecutionSettings.POLICY_RETRY) {
                if (attempt > 0) {
                    DebugLogger.w("WorkflowExecutor", "步骤执行失败，正在进行第 $attempt 次重试 (等待 ${retryInterval}ms)...")
                    // 在模块内部进度更新时，也刷新通知
                    ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Running(progress, "重试 ($attempt/$retryCount): ${module.metadata.name}"))
                    delay(retryInterval)
                }

                finalResult = preExecutionFailure ?: try {
                    module.execute(executionContext) { progressUpdate ->
                        DebugLogger.d("WorkflowExecutor", "[进度] ${module.metadata.name}: ${progressUpdate.message}")
                        ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Running(progress, progressUpdate.message))
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    DebugLogger.e("WorkflowExecutor", "模块执行异常: ${module.metadata.name}", e)
                    executionExceptionToFailure(e)
                }

                if (finalResult is ExecutionResult.Success || finalResult is ExecutionResult.Signal) {
                    break // 成功，跳出重试循环
                }

                // 只有策略是 RETRY 且是 Failure 时才继续循环
                if (errorPolicy == ActionStepExecutionSettings.POLICY_RETRY && finalResult is ExecutionResult.Failure) {
                    attempt++
                    if (attempt > retryCount) break
                } else {
                    break // 不是 RETRY 策略，直接处理结果
                }
            }

            // --- 结果处理 ---
            when (val result = finalResult) {
                is ExecutionResult.Success -> {
                    if (result.outputs.isNotEmpty()) {
                        stepOutputs[step.id] = VObjectFactory.fromMapAny(result.outputs)
                    }
                    pc++
                }
                is ExecutionResult.Failure -> {
                    DebugLogger.e("WorkflowExecutor", "模块执行失败: ${result.errorTitle} - ${result.errorMessage}")

                    if (errorPolicy == ActionStepExecutionSettings.POLICY_SKIP) {
                        DebugLogger.w("WorkflowExecutor", "根据策略，跳过错误继续执行。")

                        // 1. 如果模块提供了 partialOutputs，使用它们作为基础
                        // 2. 否则生成默认的 VNull 输出
                        val baseOutputs = if (result.partialOutputs.isNotEmpty()) {
                            // 使用模块提供的部分输出，转换为 VObject
                            VObjectFactory.fromMapAny(result.partialOutputs)
                        } else {
                            // 生成默认的 VNull 输出
                            generateDefaultOutputs(module, step)
                        }

                        // 3. 确保所有定义的输出都有值（没有在 partialOutputs 中的填 VNull）
                        val outputDefs = module.getOutputs(step)
                        val skipOutputs = baseOutputs.toMutableMap()
                        for (def in outputDefs) {
                            if (!skipOutputs.containsKey(def.id)) {
                                skipOutputs[def.id] = VNull
                            }
                        }

                        // 4. 添加错误元数据
                        skipOutputs.apply {
                            put("error", VString(result.errorMessage))
                            put("success", VBoolean(false))
                        }

                        stepOutputs[step.id] = skipOutputs
                        pc++ // 继续下一步
                    } else {
                        // POLICY_STOP (默认) 或 重试耗尽
                        ExecutionNotificationManager.updateState(workflow, ExecutionNotificationState.Cancelled("失败: ${result.errorMessage}"))

                        // 尝试获取 UI 服务并显示错误弹窗
                        // 仅当应用在前台或有悬浮窗权限时，弹窗才会显示（由 ExecutionUIService 处理）
                        try {
                            val localizedAppContext = LocaleManager.applyLanguage(
                                initialContext.applicationContext,
                                LocaleManager.getLanguage(initialContext.applicationContext)
                            )
                            val uiService = initialContext.services.get(ExecutionUIService::class)
                            uiService?.showError(
                                workflowName = workflow.name,
                                moduleName = localizedAppContext.getString(
                                    R.string.execution_error_step_module_name,
                                    pc + 1,
                                    module.metadata.getLocalizedName(localizedAppContext)
                                ),
                                errorMessage = result.errorMessage
                            )
                        } catch (e: Exception) {
                            DebugLogger.e("WorkflowExecutor", "显示错误弹窗失败", e)
                        }

                        // 获取完整日志并广播失败状态
                        val fullLog = executionLogs[initialContext.workflowStack.first()]?.toString() ?: ""

                        val hasActiveExecutions = unregisterExecution(workflow.id, executionInstanceId)
                        failedExecutions[executionInstanceId] = "${result.errorTitle}: ${result.errorMessage}"
                        stoppedWorkflows.remove(executionInstanceId)
                        if (!hasActiveExecutions) {
                            executionLogs.remove(workflow.id)
                        }
                        ExecutionStateBus.postState(
                            ExecutionState.Failure(
                                workflowId = workflow.id,
                                executionInstanceId = executionInstanceId,
                                stepIndex = pc,
                                detailedLog = fullLog
                            )
                        )
                        return null
                    }
                }
                is ExecutionResult.Signal -> {
                    DebugLogger.d("WorkflowExecutor", "信号: ${result.signal}")
                    when (val signal = result.signal) {
                        is ExecutionSignal.Jump -> {
                            pc = signal.pc
                            continue
                        }
                        is ExecutionSignal.Loop -> {
                            when (signal.action) {
                                LoopAction.START -> pc++ // 循环开始，进入循环体第一个模块
                                LoopAction.END -> {
                                    val loopState = loopStack.peek() as? LoopState.CountLoopState
                                    if (loopState != null && loopState.currentIteration < loopState.totalIterations) {
                                        val loopStartPos = BlockNavigator.findBlockStartPosition(workflow.steps, pc, LOOP_START_ID)
                                        if (loopStartPos != -1) {
                                            pc = loopStartPos + 1
                                        } else {
                                            DebugLogger.w("WorkflowExecutor", "找不到循环起点，异常退出循环。")
                                            pc++
                                        }
                                    } else {
                                        loopStack.pop() // 循环结束
                                        pc++
                                    }
                                }
                            }
                        }
                        is ExecutionSignal.Break -> {
                            val currentLoopPairingId = BlockNavigator.findCurrentLoopPairingId(workflow.steps, pc)
                            val endBlockPosition = BlockNavigator.findEndBlockPosition(workflow.steps, pc, currentLoopPairingId)
                            if (endBlockPosition != -1) {
                                pc = endBlockPosition + 1
                                DebugLogger.d("WorkflowExecutor", "接收到Break信号，跳出循环 '$currentLoopPairingId' 到步骤 $pc")
                            } else {
                                DebugLogger.w("WorkflowExecutor", "接收到Break信号，但找不到匹配的结束循环块。")
                                pc++
                            }
                        }
                        is ExecutionSignal.Continue -> {
                            val loopStartPos = BlockNavigator.findCurrentLoopStartPosition(workflow.steps, pc)
                            if (loopStartPos != -1) {
                                val loopModule = ModuleRegistry.getModule(workflow.steps[loopStartPos].moduleId)
                                val endBlockPos = BlockNavigator.findEndBlockPosition(workflow.steps, loopStartPos, loopModule?.blockBehavior?.pairingId)
                                if (endBlockPos != -1) {
                                    pc = endBlockPos
                                } else {
                                    pc++
                                }
                                DebugLogger.d("WorkflowExecutor", "接收到Continue信号，跳转到步骤 $pc")
                            } else {
                                DebugLogger.w("WorkflowExecutor", "接收到Continue信号，但找不到循环起点。")
                                pc++
                            }
                        }
                        is ExecutionSignal.Stop -> {
                            DebugLogger.d("WorkflowExecutor", "接收到Stop信号，正常终止工作流。")
                            stoppedWorkflows[executionInstanceId] = true
                            pc = workflow.steps.size // 设置pc越界以跳出主循环
                        }
                        // 处理 Return 信号
                        is ExecutionSignal.Return -> {
                            DebugLogger.d("WorkflowExecutor", "接收到Return信号，返回值: ${signal.result}")
                            returnValue = signal.result
                            pc = workflow.steps.size // 立即结束当前工作流的执行
                        }
                    }
                }
                null -> {
                    // 理论不应发生
                    pc++
                }
            }
        }
        return returnValue // 返回子工作流的结果
    }

    private fun executionExceptionToFailure(error: Exception): ExecutionResult.Failure {
        return when (error) {
            is InlineScriptEvaluator.InlineScriptExecutionException -> {
                ExecutionResult.Failure(
                    errorTitle = "Inline JavaScript 执行失败",
                    errorMessage = formatExecutionExceptionMessage(error, "Inline JavaScript 执行失败")
                )
            }
            else -> {
                ExecutionResult.Failure(
                    errorTitle = "模块执行异常",
                    errorMessage = formatExecutionExceptionMessage(error, "模块执行异常")
                )
            }
        }
    }

    private fun formatExecutionExceptionMessage(error: Throwable, fallback: String): String {
        return error.localizedMessage ?: error.message ?: fallback
    }

    /**
     * 为跳过的模块生成默认的空输出值。
     * 避免魔法变量在解析时因找不到值而回退到原始字符串。
     */
    private fun generateDefaultOutputs(module: ActionModule, step: ActionStep): Map<String, VObject> {
        val outputs = mutableMapOf<String, VObject>()
        try {
            // 获取模块定义的所有输出
            val outputDefs = module.getOutputs(step)
            for (def in outputDefs) {
                // 统一返回 VNull，表示"操作失败，没有值"
                // 这样用户可以通过 IF 模块的"不存在"操作符来检测失败
                // 参考：EXCEPTION_HANDLING.md 和 VOBJECT_SEMANTICS.md
                outputs[def.id] = VNull
            }
        } catch (e: Exception) {
            DebugLogger.w("WorkflowExecutor", "生成默认输出时出错: ${e.message}")
        }
        return outputs
    }

    private fun registerExecution(workflowId: String, instanceId: String, job: Job) {
        runningWorkflows.compute(workflowId) { _, existing ->
            val executions = existing ?: mutableListOf()
            executions.add(ActiveExecution(instanceId, job))
            executions
        }
    }

    private fun unregisterExecution(workflowId: String, instanceId: String): Boolean {
        var hasRemaining = false
        runningWorkflows.computeIfPresent(workflowId) { _, existing ->
            existing.removeAll { it.instanceId == instanceId }
            hasRemaining = existing.isNotEmpty()
            if (existing.isEmpty()) null else existing
        }
        return hasRemaining
    }

    private fun addReentryLog(workflow: Workflow, messageKey: LogMessageKey) {
        LogManager.addLog(
            LogEntry(
                workflowId = workflow.id,
                workflowName = workflow.name,
                timestamp = System.currentTimeMillis(),
                status = LogStatus.CANCELLED,
                messageKey = messageKey
            )
        )
    }
}
