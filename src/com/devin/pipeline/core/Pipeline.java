package com.devin.pipeline.core;

import com.devin.pipeline.core.control.CancelToken;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.control.ErrorHandler;
import com.devin.pipeline.core.control.Interceptor;
import com.devin.pipeline.core.control.RetryPolicy;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * The engine: an immutable, thread safe description of a workflow that can be executed many times,
 * by many threads, concurrently.
 *
 * <p>One execution always runs on a single thread (the opt-in {@link ParallelStage} being the
 * documented exception), so stages, interceptors, error handling, trace capture and exporters
 * need no locks.
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

    /**
     * Runs another {@link Pipeline} as a single stage. The engine executes the child with the
     * <b>same</b> {@link StageContext}, trace, cancel token and deadline: attrs are shared,
     * child stage records land in the same trace, and abort/skip/cancel propagate to the parent.
     *
     * <p>The child pipeline's own timeout, error handler and interceptors apply to its stages;
     * its exporters and listeners are not triggered when nested.
     *
     * @param <C> payload type
     */
    public static final class SubPipelineStage<C> implements Stage<C> {

        private final String name;
        private final Pipeline<C> subPipeline;

        public SubPipelineStage(String name, Pipeline<C> subPipeline) {
            this.name = Objects.requireNonNull(name, "name");
            this.subPipeline = Objects.requireNonNull(subPipeline, "subPipeline");
        }

        @Override
        public String name() {
            return name;
        }

        public Pipeline<C> subPipeline() {
            return subPipeline;
        }

        /** Fallback used when such a stage is driven without the engine. */
        @Override
        public void handle(StageContext<C> ctx) {
            ExecutionResult<C> result = subPipeline.execute(ctx.payload());
            if (result.state() == ExecutionState.ABORTED) {
                ctx.abort(result.reason().message());
            }
        }

        @Override
        public String toString() {
            return "SubPipelineStage[" + name + " -> " + subPipeline.name() + "]";
        }
    }

    /** Per-execution bookkeeping. Never shared between executions. */
    private static final class Exec<C> {
        final ExecutionHandle<C> handle;
        final StageContext<C> ctx;
        final AtomicInteger index = new AtomicInteger();

        Exec(ExecutionHandle<C> handle, StageContext<C> ctx) {
            this.handle = handle;
            this.ctx = ctx;
        }
    }

    /** Result of running a stage list: why it stopped (if it did) and where it stopped. */
    private static final class Outcome {
        final FlowReason stop;
        final int next;

        Outcome(FlowReason stop, int next) {
            this.stop = stop;
            this.next = next;
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
    private final RetryPolicy defaultRetry;

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
                    boolean traceEnabled,
                    RetryPolicy defaultRetry) {
        this.name = Objects.requireNonNull(name, "name");
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("pipeline name must not be blank");
        }
        this.stages = List.copyOf(stages == null ? List.of() : stages);
        this.finallyStages = List.copyOf(finallyStages == null ? List.of() : finallyStages);
        this.interceptors = List.copyOf(interceptors == null ? List.of() : interceptors);
        this.errorHandler = errorHandler == null ? ErrorHandler.abortAll() : errorHandler;
        this.exporters = List.copyOf(exporters == null ? List.of() : exporters);
        this.listeners = List.copyOf(listeners == null ? List.of() : listeners);
        this.timeoutMs = timeoutMs;
        this.traceEnabled = traceEnabled;
        this.defaultRetry = defaultRetry;
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

    /** Pipeline-wide default retry policy; {@code null} means a single attempt. */
    public RetryPolicy defaultRetry() {
        return defaultRetry;
    }

    /** Runs every stage on the caller thread and returns the terminal result. */
    public ExecutionResult<C> execute(C payload) {
        Exec<C> e = newExecution(payload);
        ExecutionResult<C> result = run(e);
        e.handle.complete(result);
        return result;
    }

    /**
     * Android production requirement: async execution must be explicit.
     * The caller must provide the target executor, because implicit JVM defaults are not safe for
     * Android lifecycle-sensitive workloads.
     */
    public ExecutionHandle<C> executeAsync(C payload) {
        throw new UnsupportedOperationException(
                "Android production mode requires an explicit executor. Use executeAsync(payload, executor)."
        );
    }

    /**
     * Submits the execution to the given executor. Stages still run serially on one worker thread,
     * with semantics identical to {@link #execute(Object)}.
     *
     * <p>If the executor rejects the task, or the engine itself dies with an unexpected
     * {@code Error} (OOM and the like), the returned handle completes exceptionally instead of
     * staying pending forever.
     */
    public ExecutionHandle<C> executeAsync(C payload, Executor executor) {
        Objects.requireNonNull(executor, "executor");
        Exec<C> e = newExecution(payload);
        try {
            executor.execute(() -> {
                ExecutionResult<C> result;
                try {
                    result = run(e);
                } catch (Throwable t) {
                    failHandle(e, t);
                    return;
                }
                e.handle.complete(result);
            });
        } catch (RuntimeException | Error t) {
            failHandle(e, t);
            throw t;
        }
        return e.handle;
    }

    /** Terminal fallback for engine-level failures: state FAILED, future completed exceptionally. */
    private void failHandle(Exec<C> e, Throwable t) {
        transition(e, ExecutionState.FAILED, FlowReason.failed(describe(t)));
        e.handle.markStage(null);
        e.handle.markIteration(0);
        e.handle.completeExceptionally(t);
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
        long deadline = Long.MAX_VALUE;
        if (timeoutMs > 0) {
            deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        }

        transition(e, ExecutionState.RUNNING, FlowReason.normal("started"));
        emit(e, PipelineEvent.pipelineStart(e.handle.traceId(), name));

        Outcome outcome = runStageList(e, trace, stages, null, 0, null, deadline);
        FlowReason stop = outcome.stop;
        if (stop == null) {
            stop = checkStop(ctx, deadline);
        }
        if (stop == null) {
            stop = FlowReason.normal();
            if (trace != null && trace.truncationReason() != null) {
                stop = trace.truncationReason();
            }
        }
        if (outcome.next < stages.size()) {
            markRemaining(e, trace, outcome.next, stop);
        }

        runFinally(e, trace, null, 0);

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

    /**
     * Runs a list of stages sequentially; the single checkpoint loop of the engine.
     *
     * @param scope  record scope path ({@code null} at top level, loop/parallel name deeper)
     * @param iteration current loop iteration for records
     * @param errorPolicy error action override inherited from an enclosing loop
     * @param deadline nanoTime deadline, {@code Long.MAX_VALUE} when there is none
     */
    private Outcome runStageList(Exec<C> e, ExecutionTrace trace, List<Stage<C>> list,
                                 String scope, int iteration, ErrorAction errorPolicy, long deadline) {
        for (int i = 0; i < list.size(); i++) {
            FlowReason stop = checkStop(e.ctx, deadline);
            if (stop != null) {
                return new Outcome(stop, i);
            }
            FlowReason failed = runUnit(e, trace, list.get(i), scope, iteration, errorPolicy, deadline);
            if (failed != null) {
                return new Outcome(failed, i + 1);
            }
        }
        return new Outcome(null, list.size());
    }

    private FlowReason runUnit(Exec<C> e, ExecutionTrace trace, Stage<C> stage,
                               String scope, int iteration, ErrorAction errorPolicy, long deadline) {
        if (stage instanceof LoopStage<C> loop) {
            return runLoop(e, trace, loop, scope, errorPolicy, deadline);
        }
        if (stage instanceof SubPipelineStage<C> sub) {
            return sub.subPipeline().runNested(e, trace, scope, iteration, errorPolicy, deadline);
        }
        if (stage instanceof ParallelStage<C> parallel) {
            return runParallel(e, trace, parallel, scope, iteration, errorPolicy, deadline);
        }
        return runStage(e, trace, stage, scope, iteration, errorPolicy, deadline);
    }

    private FlowReason runStage(Exec<C> e,
                                ExecutionTrace trace,
                                Stage<C> stage,
                                String scope,
                                int iteration,
                                ErrorAction errorPolicy,
                                long deadline) {
        StageContext<C> ctx = e.ctx;
        String traceId = e.handle.traceId();
        int index = e.index.getAndIncrement();
        Instant start = Instant.now();

        ctx.setCurrentStage(stage.name());
        e.handle.markStage(stage.name());
        emit(e, PipelineEvent.stageStart(traceId, name, stage.name(), scope, iteration));

        boolean accepted;
        try {
            accepted = stage.accept(ctx);
        } catch (Throwable t) {
            Throwable failure = unwrap(t);
            Instant end = Instant.now();
            record(trace, StageRecord.failed(index, stage.name(), scope, iteration, start, end, failure));
            emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                    StageStatus.FAILED, elapsed(start, end), describe(failure)));
            return decide(ctx, stage, failure, errorPolicy) == ErrorAction.ABORT
                    ? FlowReason.failed(stage.name(), failure) : null;
        }

        if (!accepted) {
            Instant end = Instant.now();
            record(trace, StageRecord.skipped(index, stage.name(), scope, iteration, start, end));
            emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                    StageStatus.SKIPPED, elapsed(start, end), null));
            return null;
        }

        RetryPolicy retry = stage.retry() != null ? stage.retry() : defaultRetry;
        int maxAttempts = retry == null ? 1 : Math.max(1, retry.maxAttempts());

        int attempts = 0;
        Throwable failure = null;
        while (attempts < maxAttempts) {
            attempts++;
            for (Interceptor<C> interceptor : interceptors) {
                guard(() -> interceptor.beforeStage(stage, ctx));
            }
            Throwable thrown = null;
            try {
                executeBody(stage, ctx, deadline);
            } catch (Throwable t) {
                thrown = unwrap(t);
            }
            // per attempt: afterStage always pairs with the beforeStage above
            afterStage(stage, ctx);
            if (thrown == null) {
                Instant end = Instant.now();
                record(trace, StageRecord.success(index, stage.name(), scope, iteration, start, end, attempts));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                        StageStatus.SUCCESS, elapsed(start, end), null));
                return null;
            }
            failure = thrown;
            final Throwable err = thrown;
            for (Interceptor<C> interceptor : interceptors) {
                guard(() -> interceptor.onStageError(stage, ctx, err));
            }
            if (thrown instanceof DeadlineExceededException) {
                // the execution is out of time: not a business failure, no retry
                Instant end = Instant.now();
                record(trace, StageRecord.failed(index, stage.name(), scope, iteration, start, end, failure, attempts));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                        StageStatus.FAILED, elapsed(start, end), describe(failure)));
                return deadlineExceeded();
            }
            if (retry != null && retry.shouldRetry(attempts, thrown) && sleepBackoff(retry, attempts, ctx, deadline)) {
                continue;
            }
            break;
        }

        Instant end = Instant.now();
        record(trace, StageRecord.failed(index, stage.name(), scope, iteration, start, end, failure, attempts));
        emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                StageStatus.FAILED, elapsed(start, end), describe(failure)));
        if (deadline != Long.MAX_VALUE && System.nanoTime() >= deadline) {
            // retries may have eaten the remaining time: the deadline is what ended this stage
            return deadlineExceeded();
        }
        return decide(ctx, stage, failure, errorPolicy) == ErrorAction.ABORT
                ? FlowReason.failed(stage.name(), failure) : null;
    }

    private FlowReason runLoop(Exec<C> e, ExecutionTrace trace, LoopStage<C> loop,
                               String scope, ErrorAction outerPolicy, long deadline) {
        StageContext<C> ctx = e.ctx;
        ErrorAction policy = loop.iterationErrorPolicy() != null ? loop.iterationErrorPolicy() : outerPolicy;
        String loopScope = scope == null ? loop.name() : scope + "/" + loop.name();
        StageContext.LoopInfo previous = ctx.currentLoop();
        try {
            for (int iteration = 1; ; iteration++) {
                FlowReason stop = checkStop(ctx, deadline);
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
                        trace.markTruncated(FlowReason.loopTruncated(loop.maxIterations()));
                    }
                    return null;
                }
                ctx.enterLoop(loop.name(), iteration);
                e.handle.markIteration(iteration);
                try {
                    for (Stage<C> body : loop.body()) {
                        stop = checkStop(ctx, deadline);
                        if (stop != null) {
                            return stop;
                        }
                        FlowReason failed = runUnit(e, trace, body, loopScope, iteration, policy, deadline);
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

    private FlowReason runParallel(Exec<C> e, ExecutionTrace trace, ParallelStage<C> parallel,
                                   String scope, int iteration, ErrorAction errorPolicy, long deadline) {
        String branchScope = scope == null ? parallel.name() : scope + "/" + parallel.name();
        Executor executor = parallel.executor();
        List<CompletableFuture<FlowReason>> futures = new ArrayList<>();
        for (Stage<C> branch : parallel.branches()) {
            CompletableFuture<FlowReason> future = CompletableFuture.supplyAsync(() -> {
                try {
                    FlowReason stop = checkStop(e.ctx, deadline);
                    if (stop != null) {
                        return stop;
                    }
                    return runUnit(e, trace, branch, branchScope, iteration, errorPolicy, deadline);
                } catch (Error error) {
                    throw error; // OOM & friends must not be downgraded to a stage failure
                } catch (Throwable t) {
                    return FlowReason.failed(branch.name(), unwrap(t));
                }
            }, executor);
            futures.add(future);
        }
        if (parallel.failFast()) {
            // as soon as one branch stops the execution, cancel the siblings
            for (CompletableFuture<FlowReason> future : futures) {
                future.whenComplete((reason, error) -> {
                    if (reason != null || error != null) {
                        for (CompletableFuture<FlowReason> other : futures) {
                            if (other != future) {
                                other.cancel(true);
                            }
                        }
                    }
                });
            }
        }
        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            if (deadline == Long.MAX_VALUE) {
                all.join();
            } else {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    cancelAll(futures);
                    return deadlineExceeded();
                }
                all.get(remainingNanos, TimeUnit.NANOSECONDS);
            }
        } catch (java.util.concurrent.TimeoutException te) {
            cancelAll(futures);
            return deadlineExceeded();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            cancelAll(futures);
            return FlowReason.cancelled("thread interrupted");
        } catch (java.util.concurrent.CancellationException ce) {
            // fail-fast cancelled the siblings: the aggregation below still sees the real reason
        } catch (ExecutionException | CompletionException ex) {
            Throwable cause = unwrap(ex);
            if (!(cause instanceof java.util.concurrent.CancellationException)) {
                cancelAll(futures);
                if (cause instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException("parallel stage '" + parallel.name()
                        + "' failed unexpectedly", cause);
            }
            // fail-fast cancelled a sibling: the aggregation below still sees the real reason
        }
        // aggregate: the first reason wins, later ones are appended so nothing is lost silently
        FlowReason first = null;
        List<FlowReason> others = null;
        for (CompletableFuture<FlowReason> future : futures) {
            FlowReason reason;
            try {
                reason = future.join();
            } catch (java.util.concurrent.CancellationException ce) {
                reason = FlowReason.cancelled("branch cancelled (fail-fast)");
            }
            if (reason != null) {
                if (first == null) {
                    first = reason;
                } else {
                    if (others == null) {
                        others = new ArrayList<>();
                    }
                    others.add(reason);
                }
            }
        }
        if (first == null) {
            return null;
        }
        if (others == null) {
            return first;
        }
        StringBuilder message = new StringBuilder(first.message());
        for (FlowReason other : others) {
            message.append("; also: ").append(other);
        }
        return FlowReason.of(first.type(), first.code(), message.toString());
    }

    /**
     * Runs a child pipeline inside this execution: same context, trace, cancel token and
     * (narrowed) deadline. Called by {@link SubPipelineStage} handling in {@link #runUnit}.
     */
    FlowReason runNested(Exec<C> e, ExecutionTrace trace, String scope, int iteration,
                         ErrorAction inheritedPolicy, long outerDeadline) {
        long deadline = outerDeadline;
        if (timeoutMs > 0) {
            long own = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            deadline = Math.min(outerDeadline, own);
        }
        Outcome outcome = runStageList(e, trace, stages, scope, iteration, inheritedPolicy, deadline);
        FlowReason stop = outcome.stop;
        if (stop == null) {
            stop = checkStop(e.ctx, deadline);
        }
        runFinally(e, trace, scope, iteration);
        return stop;
    }

    private void runFinally(Exec<C> e, ExecutionTrace trace, String scope, int iteration) {
        StageContext<C> ctx = e.ctx;
        String traceId = e.handle.traceId();
        for (Stage<C> stage : finallyStages) {
            Instant start = Instant.now();
            int index = e.index.getAndIncrement();
            Throwable failure = null;
            boolean accepted = true;
            try {
                accepted = stage.accept(ctx);
                if (accepted) {
                    // cleanup must run on every exit path: no deadline enforcement here
                    executeBody(stage, ctx, Long.MAX_VALUE);
                }
            } catch (Throwable t) {
                failure = unwrap(t);
            }
            Instant end = Instant.now();
            if (failure != null) {
                record(trace, StageRecord.failed(index, stage.name(), scope, iteration, start, end, failure));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                        StageStatus.FAILED, elapsed(start, end), describe(failure)));
            } else if (accepted) {
                record(trace, StageRecord.success(index, stage.name(), scope, iteration, start, end));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
                        StageStatus.SUCCESS, elapsed(start, end), null));
            } else {
                record(trace, StageRecord.skipped(index, stage.name(), scope, iteration, start, end));
                emit(e, PipelineEvent.stageFinish(traceId, name, stage.name(), scope, iteration,
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
            int index = e.index.getAndIncrement();
            record(trace, cancelled ? StageRecord.cancelled(index, stage.name(), null, 0, now, now)
                    : StageRecord.skipped(index, stage.name(), null, 0, now, now));
            emit(e, PipelineEvent.stageFinish(e.handle.traceId(), name, stage.name(), null, 0,
                    status, 0L, null));
        }
    }

    private void executeBody(Stage<C> stage, StageContext<C> ctx, long deadline) throws Exception {
        if (stage instanceof AsyncStage<C> asyncStage) {
            CompletableFuture<Void> future = asyncStage.handleAsync(ctx);
            if (future != null) {
                joinAsync(future, ctx, deadline);
            }
            return;
        }
        stage.handle(ctx);
    }

    /**
     * Joins an async stage body in small slices so the join stays responsive to cancellation and
     * the deadline; on cancel or deadline the underlying future is cancelled and a recognizable
     * failure is thrown. {@code DeadlineExceededException} turns the whole execution into TIMEOUT.
     */
    private void joinAsync(CompletableFuture<Void> future, StageContext<C> ctx, long deadline)
            throws Exception {
        final long sliceMs = 100L;
        while (true) {
            if (future.isDone()) {
                future.join();
                return;
            }
            if (ctx.cancelToken().isCancelled()) {
                future.cancel(true);
                throw new IllegalStateException("execution cancelled while waiting for async stage");
            }
            if (Thread.currentThread().isInterrupted()) {
                future.cancel(true);
                throw new InterruptedException("thread interrupted while waiting for async stage");
            }
            if (deadline != Long.MAX_VALUE && System.nanoTime() >= deadline) {
                future.cancel(true);
                throw new DeadlineExceededException("execution deadline exceeded while waiting for async stage");
            }
            try {
                if (deadline == Long.MAX_VALUE) {
                    future.get(sliceMs, TimeUnit.MILLISECONDS);
                } else {
                    long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                    if (remaining <= 0) {
                        future.cancel(true);
                        throw new DeadlineExceededException("execution deadline exceeded while waiting for async stage");
                    }
                    future.get(Math.min(sliceMs, remaining), TimeUnit.MILLISECONDS);
                }
                return;
            } catch (java.util.concurrent.TimeoutException te) {
                // slice elapsed: re-check cancel / deadline above
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                future.cancel(true);
                throw ie;
            } catch (ExecutionException ee) {
                Throwable cause = unwrap(ee);
                if (cause instanceof Error error) {
                    throw error;
                }
                if (cause instanceof Exception ex) {
                    throw ex;
                }
                throw ee;
            }
        }
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
            return deadlineExceeded();
        }
        return null;
    }

    /** Signals an async stage missed the execution deadline; not a business failure. */
    static final class DeadlineExceededException extends RuntimeException {
        DeadlineExceededException(String message) {
            super(message);
        }
    }

    /** TIMEOUT reason reporting how far past the deadline the engine noticed. */
    private static FlowReason deadlineExceeded() {
        return FlowReason.of(FlowReason.Type.TIMEOUT, "TIMEOUT", "execution deadline exceeded");
    }

    private static void cancelAll(List<CompletableFuture<FlowReason>> futures) {
        for (CompletableFuture<FlowReason> future : futures) {
            future.cancel(true);
        }
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

    /** Backoff sleep in small slices so cancel/interrupt/deadline stay responsive; false = do not retry. */
    private boolean sleepBackoff(RetryPolicy retry, int failedAttempt, StageContext<C> ctx, long deadline) {
        long remaining = retry.delayForAttempt(failedAttempt);
        long step = 50L;
        while (remaining > 0) {
            if (ctx.cancelToken().isCancelled() || Thread.currentThread().isInterrupted()) {
                return false;
            }
            if (deadline != Long.MAX_VALUE && System.nanoTime() >= deadline) {
                return false;
            }
            long slice = Math.min(step, remaining);
            try {
                Thread.sleep(slice);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
            remaining -= slice;
        }
        return true;
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
