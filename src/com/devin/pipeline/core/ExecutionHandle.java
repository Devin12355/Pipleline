package com.devin.pipeline.core;

import com.devin.pipeline.core.control.CancelToken;
import com.devin.pipeline.core.execution.ExecutionResult;
import com.devin.pipeline.core.execution.ExecutionState;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runtime handle of one execution: the async submitter's window into a running pipeline.
 *
 * <p>All read methods are volatile snapshot reads, safe to poll from another thread.
 *
 * @param <C> payload type
 */
public final class ExecutionHandle<C> {

    private final String traceId;
    private final String pipelineName;
    private final C payload;
    private final CancelToken cancelToken;
    private final CompletableFuture<ExecutionResult<C>> future = new CompletableFuture<>();
    private final Instant submitTime = Instant.now();

    private volatile ExecutionState state = ExecutionState.PENDING;
    private volatile String currentStage;
    private volatile int iteration;

    ExecutionHandle(String traceId, String pipelineName, C payload, CancelToken cancelToken) {
        this.traceId = traceId;
        this.pipelineName = pipelineName;
        this.payload = payload;
        this.cancelToken = cancelToken;
    }

    public String traceId() {
        return traceId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    /** Live payload reference; readable at any time, but honest only for the caller thread. */
    public C payload() {
        return payload;
    }

    public ExecutionState state() {
        return state;
    }

    /** Stage currently executing, {@code null} before the first stage and after the last. */
    public String currentStage() {
        return currentStage;
    }

    /** Current loop iteration, 0 when not inside a loop. */
    public int iteration() {
        return iteration;
    }

    public Instant submitTime() {
        return submitTime;
    }

    public CancelToken cancelToken() {
        return cancelToken;
    }

    public CompletableFuture<ExecutionResult<C>> future() {
        return future;
    }

    public boolean done() {
        return future.isDone();
    }

    /** Cooperative cancellation: effective at the next stage boundary. */
    public void cancel() {
        cancel("cancelled by handle");
    }

    public void cancel(String reason) {
        cancelToken.cancel(reason);
    }

    /** Waits for the terminal result; cancellation timeouts surface as normal results. */
    public ExecutionResult<C> await() {
        return future.join();
    }

    /** Waits at most the given time; returns {@code null} on timeout. */
    public ExecutionResult<C> await(long timeout, TimeUnit unit) {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | TimeoutException e) {
            return null;
        }
    }

    void markState(ExecutionState newState) {
        this.state = newState;
    }

    void markStage(String stageName) {
        this.currentStage = stageName;
    }

    void markIteration(int iteration) {
        this.iteration = iteration;
    }

    void complete(ExecutionResult<C> result) {
        future.complete(result);
    }

    void completeExceptionally(Throwable error) {
        future.completeExceptionally(error);
    }

    @Override
    public String toString() {
        return "ExecutionHandle{id=" + traceId
                + ", pipeline=" + pipelineName
                + ", state=" + state
                + ", stage=" + currentStage
                + ", iteration=" + iteration
                + '}';
    }
}
