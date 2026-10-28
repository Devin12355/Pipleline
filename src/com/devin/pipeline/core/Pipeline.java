package com.devin.pipeline.core;

import com.devin.pipeline.core.control.CancelToken;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.control.ErrorHandler;
import com.devin.pipeline.core.control.Interceptor;
import com.devin.pipeline.core.event.PipelineEvent;
import com.devin.pipeline.core.event.PipelineListener;
import com.devin.pipeline.core.execution.ExecutionResult;
import com.devin.pipeline.core.execution.ExecutionState;
import com.devin.pipeline.core.execution.FlowReason;
import com.devin.pipeline.core.execution.StageStatus;
import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.core.trace.StageRecord;
import com.devin.pipeline.core.trace.TraceExporter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * The engine: an immutable, thread safe description of a workflow that can be executed many times,
 * by many threads, concurrently.
 *
 * <p>One execution always runs on a single thread: stages, interceptors, error handling, trace
 * capture and exporters all run there, so no locks are needed anywhere.
 *
 * @param <C> payload type
 */
public final class Pipeline<C> {

    /**
     * A loop is still a stage from the builder's point of view, but the engine recognizes it and
     * runs its body through the very same execution loop, giving each stage of each iteration its
     * own trace record.
     *
     * @param <C> payload type
     */
    public static final class LoopStage<C> implements Stage<C> {

        private final String name;
        private final List<Stage<C>> body;
        private final Predicate<StageContext<C>> condition;
        private final int maxIterations;
        private final ErrorAction iterationErrorPolicy;

        public LoopStage(String name,
                         List<Stage<C>> body,
                         Predicate<StageContext<C>> condition,
                         int maxIterations,
                         ErrorAction iterationErrorPolicy) {
            if (body == null || body.isEmpty()) {
                throw new IllegalArgumentException("loop '" + name + "' needs at least one stage");
            }
            this.name = Objects.requireNonNull(name, "name");
            this.body = List.copyOf(body);
            this.condition = condition == null ? ctx -> true : condition;
            this.maxIterations = maxIterations > 0 ? maxIterations : Integer.MAX_VALUE;
            this.iterationErrorPolicy = iterationErrorPolicy;
        }

        @Override
        public String name() {
            return name;
        }

        public List<Stage<C>> body() {
            return body;
        }

        public Predicate<StageContext<C>> condition() {
            return condition;
        }

        public int maxIterations() {
            return maxIterations;
        }

        /** Error policy used inside the loop; {@code null} means inherit the pipeline policy. */
        public ErrorAction iterationErrorPolicy() {
            return iterationErrorPolicy;
        }

        /**
         * Best effort fallback when a loop is driven without the engine: the body is executed
         * inline, honouring condition, iteration bound and {@code exitLoop()}.
         */
        @Override
        public void handle(StageContext<C> ctx) throws Exception {
            for (int iteration = 1; iteration <= maxIterations; iteration++) {
                if (!condition.test(ctx)) {
                    return;
                }
                for (Stage<C> stage : body) {
                    if (ctx.stopRequested()) {
                        return;
                    }
                    if (stage.accept(ctx)) {
                        stage.handle(ctx);
                    }
                }
                if (ctx.isExitLoop()) {
                    ctx.resetExitLoop();
                    return;
                }
            }
        }

        @Override
        public String toString() {
            return "LoopStage[" + name + ", stages=" + body.size() + "]";
        }
    }

    /** Per-execution bookkeeping. Never shared between executions. */
    private static final class Exec<C> {
        final ExecutionHandle<C> handle;
        final StageContext<C> ctx;
        int index;
        long deadline = Long.MAX_VALUE;

        Exec(ExecutionHandle<C> handle, StageContext<C> ctx) {
            this.handle = handle;
            this.ctx = ctx;
        }
    }

    private static final AtomicLong SEQ = new AtomicLong();

    private final String name;
    private final List<Stage<C>> stages;
    private final List<Stage<C>> finallyStages;
    private final List<Interceptor<C>> interceptors;
    private final ErrorHandler<C> errorHandler;
    private final List<TraceExporter> exporters;
    private final List<PipelineListener<C>> listeners;
    private final long timeoutMs;
    private final boolean traceEnabled;

    /**
     * Called by {@code PipelineBuilder}. Instances are immutable afterwards and safe to share
     * between threads.
     */
    public Pipeline(String name,
                    List<Stage<C>> stages,
                    List<Stage<C>> finallyStages,
                    List<Interceptor<C>> interceptors,
                    ErrorHandler<C> errorHandler,
                    List<TraceExporter> exporters,
                    List<PipelineListener<C>> listeners,
                    long timeoutMs,
                    boolean traceEnabled) {
        this.name = Objects.requireNonNull(name, "name");
        this.stages = List.copyOf(stages == null ? List.of() : stages);
        this.finallyStages = List.copyOf(finallyStages == null ? List.of() : finallyStages);
        this.interceptors = List.copyOf(interceptors == null ? List.of() : interceptors);
        this.errorHandler = errorHandler == null ? ErrorHandler.abortAll() : errorHandler;
        this.exporters = List.copyOf(exporters == null ? List.of() : exporters);
        this.listeners = List.copyOf(listeners == null ? List.of() : listeners);
        this.timeoutMs = timeoutMs;
        this.traceEnabled = traceEnabled;
    }

    public String name() {
        return name;
    }

    public List<Stage<C>> stages() {
        return stages;
    }

    public List<Stage<C>> finallyStages() {
        return finallyStages;
    }

    public List<Interceptor<C>> interceptors() {
        return interceptors;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public boolean traceEnabled() {
        return traceEnabled;
    }

    /** Runs every stage on the caller thread and returns the terminal result. */
    public ExecutionResult<C> execute(C payload) {
        Exec<C> e = newExecution(payload);
        ExecutionResult<C> result = run(e);
        e.handle.complete(result);
        return result;
    }

    /** Submits the execution to the common pool and returns immediately. */
    public ExecutionHandle<C> executeAsync(C payload) {
        return executeAsync(payload, ForkJoinPool.commonPool());
    }

    /**
     * Submits the execution to the given executor. Stages still run serially on one worker thread,
     * with semantics identical to {@link #execute(Object)}.
     */
    public ExecutionHandle<C> executeAsync(C payload, Executor executor) {
        Objects.requireNonNull(executor, "executor");
        Exec<C> e = newExecution(payload);
        executor.execute(() -> {
            ExecutionResult<C> result = run(e);
            e.handle.complete(result);
        });
        return e.handle;
    }

    // ---- execution -------------------------------------------------------------------------

    private Exec<C> newExecution(C payload) {
        String traceId = newTraceId();
        CancelToken token = new CancelToken();
        ExecutionHandle<C> handle = new ExecutionHandle<>(traceId, name, payload, token);
        StageContext<C> ctx = new StageContext<>(name, traceId, payload, token);
        return new Exec<>(handle, ctx);
    }

    private String newTraceId() {
        String safe = name.replaceAll("[^A-Za-z0-9._-]", "-");
        return safe + "-" + System.currentTimeMillis() + "-" + SEQ.incrementAndGet();
    }

    private ExecutionResult<C> run(Exec<C> e) {
        StageContext<C> ctx = e.ctx;
        Instant start = Instant.now();
        ExecutionTrace trace = traceEnabled ? new ExecutionTrace(e.handle.traceId(), name, start) : null;
        if (timeoutMs > 0) {
            e.deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        }

        transition(e, ExecutionState.RUNNING, FlowReason.normal("started"));
        emit(e, PipelineEvent.pipelineStart(e.handle.traceId(), name));

        FlowReason stop = null;
        int pending = 0;
        for (int i = 0; i < stages.size(); i++) {
            stop = checkStop(ctx, e.deadline);
            if (stop != null) {
                pending = i;
                break;
            }
            pending = i + 1;
            FlowReason failed = runUnit(e, trace, stages.get(i), null, 0, null);
            if (failed != null) {
                stop = failed;
                break;
            }
        }
        if (stop == null) {
            stop = checkStop(ctx, e.deadline);
        }
        if (stop == null) {
            stop = FlowReason.normal();
        }
        if (pending < stages.size()) {
            markRemaining(e, trace, pending, stop);
        }

        runFinally(e, trace);

        ExecutionState terminal = terminalOf(stop);
        Instant end = Instant.now();
        if (trace != null) {
            trace.complete(stop, end);
        }
        export(trace);
        emit(e, PipelineEvent.pipelineFinish(e.handle.traceId(), name, terminal, stop,
                elapsed(start, end)));
        transition(e, terminal, stop);
        e.handle.markStage(null);
        e.handle.markIteration(0);
        return new ExecutionResult<>(e.handle.traceId(), name, terminal, stop, ctx.payload(), trace,
                start, end);
    }

    private FlowReason runUnit(Exec<C> e,
                               ExecutionTrace trace,
                               Stage<C> stage,
                               String loopName,
                               int iteration,
                               ErrorAction outerPolicy) {
        if (stage instanceof LoopStage<C> loop) {
            return runLoop(e, trace, loop, outerPolicy);
        }
        return runStage(e, trace, stage, loopName, iteration, outerPolicy);
    }

    private FlowReason runStage(Exec<C> e,
                                ExecutionTrace trace,
                                Stage<C> stage,
                                String loopName,
                                int iteration,
                                ErrorAction errorPolicy) {
        StageContext<C> ctx = e.ctx;
        String traceId = e.handle.traceId();
        int index = e.index++;
        Instant start = Instant.now();

        ctx.setCurrentStage(stage.name());
        e.handle.markStage(stage.name());
        emit(e, PipelineEvent.stageStart(traceId, name, stage.name(), loopName, iteration));

        boolean accepted;
        try {
            accepted = stage.accept(ctx);
        } catch (Throwable t) {
            Throwable failure = unwrap(t);
            Instant end = Instant.now();
            record(trace, StageRecord.failed(index, stage.name(), loopName, iteration, start, end, failure));
            emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), loopName, iteration,
                    StageStatus.FAILED, elapsed(start, end), describe(failure)));
            return decide(ctx, stage, failure, errorPolicy) == ErrorAction.ABORT
                    ? FlowReason.failed(stage.name(), failure) : null;
        }

        if (!accepted) {
            Instant end = Instant.now();
            record(trace, StageRecord.skipped(index, stage.name(), loopName, iteration, start, end));
            emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), loopName, iteration,
                    StageStatus.SKIPPED, elapsed(start, end), null));
            return null;
        }

        Throwable thrown = null;
        for (Interceptor<C> interceptor : interceptors) {
            guard(() -> interceptor.beforeStage(stage, ctx));
        }
        try {
            executeBody(stage, ctx);
        } catch (Throwable t) {
            thrown = unwrap(t);
        }
        final Throwable failure = thrown;
        Instant end = Instant.now();

        if (failure != null) {
            for (Interceptor<C> interceptor : interceptors) {
                guard(() -> interceptor.onStageError(stage, ctx, failure));
            }
            record(trace, StageRecord.failed(index, stage.name(), loopName, iteration, start, end, failure));
            emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), loopName, iteration,
                    StageStatus.FAILED, elapsed(start, end), describe(failure)));
            afterStage(stage, ctx);
            return decide(ctx, stage, failure, errorPolicy) == ErrorAction.ABORT
                    ? FlowReason.failed(stage.name(), failure) : null;
        }

        afterStage(stage, ctx);
        record(trace, StageRecord.success(index, stage.name(), loopName, iteration, start, end));
        emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), loopName, iteration,
                StageStatus.SUCCESS, elapsed(start, end), null));
        return null;
    }

    private FlowReason runLoop(Exec<C> e, ExecutionTrace trace, LoopStage<C> loop, ErrorAction outerPolicy) {
        StageContext<C> ctx = e.ctx;
        ErrorAction policy = loop.iterationErrorPolicy() != null ? loop.iterationErrorPolicy() : outerPolicy;
        StageContext.LoopInfo previous = ctx.currentLoop();
        try {
            for (int iteration = 1; ; iteration++) {
                FlowReason stop = checkStop(ctx, e.deadline);
                if (stop != null) {
                    return stop;
                }
                boolean proceed;
                try {
                    proceed = loop.condition().test(ctx);
                } catch (Throwable t) {
                    return FlowReason.failed(loop.name(), unwrap(t));
                }
                if (!proceed) {
                    return null;
                }
                if (iteration > loop.maxIterations()) {
                    if (trace != null) {
                        trace.markTruncated();
                    }
                    return null;
                }
                ctx.enterLoop(loop.name(), iteration);
                e.handle.markIteration(iteration);
                try {
                    for (Stage<C> body : loop.body()) {
                        stop = checkStop(ctx, e.deadline);
                        if (stop != null) {
                            return stop;
                        }
                        FlowReason failed = runStage(e, trace, body, loop.name(), iteration, policy);
                        if (failed != null) {
                            return failed;
                        }
                    }
                } finally {
                    ctx.restoreLoop(previous);
                    e.handle.markIteration(previous.iteration);
                }
                if (ctx.isExitLoop()) {
                    ctx.resetExitLoop();
                    return null;
                }
            }
        } finally {
            ctx.restoreLoop(previous);
        }
    }

    private void runFinally(Exec<C> e, ExecutionTrace trace) {
        StageContext<C> ctx = e.ctx;
        String traceId = e.handle.traceId();
        for (Stage<C> stage : finallyStages) {
            Instant start = Instant.now();
            int index = e.index++;
            Throwable failure = null;
            boolean accepted = true;
            try {
                accepted = stage.accept(ctx);
                if (accepted) {
                    executeBody(stage, ctx);
                }
            } catch (Throwable t) {
                failure = unwrap(t);
            }
            Instant end = Instant.now();
            if (failure != null) {
                record(trace, StageRecord.failed(index, stage.name(), null, 0, start, end, failure));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), null, 0,
                        StageStatus.FAILED, elapsed(start, end), describe(failure)));
            } else if (accepted) {
                record(trace, StageRecord.success(index, stage.name(), null, 0, start, end));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), null, 0,
                        StageStatus.SUCCESS, elapsed(start, end), null));
            } else {
                record(trace, StageRecord.skipped(index, stage.name(), null, 0, start, end));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), null, 0,
                        StageStatus.SKIPPED, elapsed(start, end), null));
            }
        }
    }

    private void markRemaining(Exec<C> e, ExecutionTrace trace, int fromIndex, FlowReason stop) {
        if (trace == null) {
            return;
        }
        boolean cancelled = stop.type() == FlowReason.Type.CANCELLED || stop.type() == FlowReason.Type.TIMEOUT;
        StageStatus status = cancelled ? StageStatus.CANCELLED : StageStatus.SKIPPED;
        Instant now = Instant.now();
        for (int i = fromIndex; i < stages.size(); i++) {
            Stage<C> stage = stages.get(i);
            int index = e.index++;
            record(trace, cancelled ? StageRecord.cancelled(index, stage.name(), null, 0, now, now)
                    : StageRecord.skipped(index, stage.name(), null, 0, now, now));
            emit(e, PipelineEvent.stageFinish(e.handle.traceId(), name, stage.name(), null, 0,
                    status, 0L, null));
        }
    }

    private void executeBody(Stage<C> stage, StageContext<C> ctx) throws Exception {
        if (stage instanceof AsyncStage<C> asyncStage) {
            CompletableFuture<Void> future = asyncStage.handleAsync(ctx);
            if (future != null) {
                future.join();
            }
            return;
        }
        stage.handle(ctx);
    }

    private FlowReason checkStop(StageContext<C> ctx, long deadline) {
        if (ctx.isAborted()) {
            return FlowReason.aborted(ctx.abortReason() == null ? "aborted" : ctx.abortReason());
        }
        if (ctx.isSkipRest()) {
            return FlowReason.normal("skipRest requested");
        }
        if (ctx.cancelToken().isCancelled()) {
            String reason = ctx.cancelToken().reason();
            return FlowReason.cancelled(reason == null ? "cancelled" : reason);
        }
        if (Thread.currentThread().isInterrupted()) {
            return FlowReason.cancelled("thread interrupted");
        }
        if (deadline != Long.MAX_VALUE && System.nanoTime() > deadline) {
            return FlowReason.timeout(timeoutMs);
        }
        return null;
    }

    private ErrorAction decide(StageContext<C> ctx, Stage<C> stage, Throwable error, ErrorAction override) {
        if (override != null) {
            return override;
        }
        try {
            ErrorAction action = errorHandler.onError(stage, ctx, error);
            return action == null ? ErrorAction.ABORT : action;
        } catch (Throwable t) {
            return ErrorAction.ABORT;
        }
    }

    private void afterStage(Stage<C> stage, StageContext<C> ctx) {
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            Interceptor<C> interceptor = interceptors.get(i);
            guard(() -> interceptor.afterStage(stage, ctx));
        }
    }

    private void record(ExecutionTrace trace, StageRecord record) {
        if (trace != null) {
            trace.add(record);
        }
    }

    private void export(ExecutionTrace trace) {
        if (trace == null) {
            return;
        }
        for (TraceExporter exporter : exporters) {
            guard(() -> exporter.export(trace));
        }
    }

    private void emit(Exec<C> e, PipelineEvent event) {
        for (PipelineListener<C> listener : listeners) {
            guard(() -> listener.onEvent(event));
        }
    }

    private void transition(Exec<C> e, ExecutionState newState, FlowReason reason) {
        ExecutionState old = e.handle.state();
        e.handle.markState(newState);
        emit(e, PipelineEvent.stateChanged(e.handle.traceId(), name, old, newState, reason));
    }

    /** Observation must never change an outcome: failures here are swallowed. */
    private void guard(Runnable action) {
        try {
            action.run();
        } catch (Throwable ignored) {
            // interceptors, listeners and exporters are diagnostics only
        }
    }

    private static ExecutionState terminalOf(FlowReason reason) {
        switch (reason.type()) {
            case ABORTED:
                return ExecutionState.ABORTED;
            case FAILED:
                return ExecutionState.FAILED;
            case CANCELLED:
                return ExecutionState.CANCELLED;
            case TIMEOUT:
                return ExecutionState.TIMEOUT;
            default:
                return ExecutionState.COMPLETED;
        }
    }

    private static long elapsed(Instant start, Instant end) {
        return end == null ? elapsed(start) : Duration.between(start, end).toMillis();
    }

    private static long elapsed(Instant start) {
        return Duration.between(start, Instant.now()).toMillis();
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            Throwable cause = t.getCause();
            return cause != null ? cause : t;
        }
        return t;
    }

    private static String describe(Throwable error) {
        if (error == null) {
            return null;
        }
        String message = error.getMessage();
        return message == null ? error.getClass().getName() : error.getClass().getName() + ": " + message;
    }

    /** Unmodifiable view kept for visitors and debugging. */
    public List<String> stageNames() {
        List<String> names = new ArrayList<>();
        for (Stage<C> stage : stages) {
            names.add(stage.name());
        }
        return Collections.unmodifiableList(names);
    }

    @Override
    public String toString() {
        return "Pipeline{name=" + name
                + ", stages=" + stages.size()
                + ", finally=" + finallyStages.size()
                + ", interceptors=" + interceptors.size()
                + ", timeoutMs=" + timeoutMs
                + ", traceEnabled=" + traceEnabled
                + '}';
    }
}
