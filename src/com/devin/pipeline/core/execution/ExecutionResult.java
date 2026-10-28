package com.devin.pipeline.core.execution;

import com.devin.pipeline.core.trace.ExecutionTrace;

import java.time.Instant;
import java.util.Objects;

/**
 * Terminal outcome of one execution: state, reason, payload and trace.
 *
 * @param <C> payload type
 */
public final class ExecutionResult<C> {

    private final String traceId;
    private final String pipelineName;
    private final ExecutionState state;
    private final FlowReason reason;
    private final C payload;
    private final ExecutionTrace trace;
    private final Instant startTime;
    private final Instant endTime;

    public ExecutionResult(String traceId,
                           String pipelineName,
                           ExecutionState state,
                           FlowReason reason,
                           C payload,
                           ExecutionTrace trace,
                           Instant startTime,
                           Instant endTime) {
        this.traceId = traceId;
        this.pipelineName = pipelineName;
        this.state = Objects.requireNonNull(state, "state");
        this.reason = reason == null ? FlowReason.normal() : reason;
        this.payload = payload;
        this.trace = trace;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String traceId() {
        return traceId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    public ExecutionState state() {
        return state;
    }

    public FlowReason reason() {
        return reason;
    }

    public C payload() {
        return payload;
    }

    /** Trace for this execution, {@code null} when tracing was disabled. */
    public ExecutionTrace trace() {
        return trace;
    }

    public Instant startTime() {
        return startTime;
    }

    public Instant endTime() {
        return endTime;
    }

    /** Wall clock duration in milliseconds, {@code -1} when unknown. */
    public long durationMs() {
        if (startTime == null || endTime == null) {
            return -1L;
        }
        return endTime.toEpochMilli() - startTime.toEpochMilli();
    }

    public boolean isSuccess() {
        return state == ExecutionState.COMPLETED && reason.isOk();
    }

    @Override
    public String toString() {
        return "ExecutionResult{id=" + traceId
                + ", pipeline=" + pipelineName
                + ", state=" + state
                + ", reason=" + reason
                + ", durationMs=" + durationMs()
                + ", stages=" + (trace == null ? 0 : trace.records().size())
                + ", payload=" + payload
                + '}';
    }
}
