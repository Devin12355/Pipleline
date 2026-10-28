package com.devin.pipeline.core.event;

import com.devin.pipeline.core.execution.ExecutionState;
import com.devin.pipeline.core.execution.FlowReason;
import com.devin.pipeline.core.execution.StageStatus;

import java.time.Instant;

/** Structured event emitted to {@link PipelineListener}s during an execution. */
public final class PipelineEvent {

    public enum Type {
        PIPELINE_START,
        STAGE_START,
        STAGE_FINISH,
        STATE_CHANGED,
        PIPELINE_FINISH
    }

    private final String traceId;
    private final String pipelineName;
    private final Type type;
    private final Instant timestamp;
    private final String stageName;
    private final String loopName;
    private final int iteration;
    private final StageStatus status;
    private final long durationMs;
    private final ExecutionState oldState;
    private final ExecutionState newState;
    private final FlowReason reason;

    private PipelineEvent(String traceId,
                          String pipelineName,
                          Type type,
                          String stageName,
                          String loopName,
                          int iteration,
                          StageStatus status,
                          long durationMs,
                          ExecutionState oldState,
                          ExecutionState newState,
                          FlowReason reason) {
        this.traceId = traceId;
        this.pipelineName = pipelineName;
        this.type = type;
        this.timestamp = Instant.now();
        this.stageName = stageName;
        this.loopName = loopName;
        this.iteration = iteration;
        this.status = status;
        this.durationMs = durationMs;
        this.oldState = oldState;
        this.newState = newState;
        this.reason = reason;
    }

    public static PipelineEvent pipelineStart(String traceId, String pipelineName) {
        return new PipelineEvent(traceId, pipelineName, Type.PIPELINE_START, null, null, 0, null,
                -1L, null, ExecutionState.PENDING, null);
    }

    public static PipelineEvent pipelineFinish(String traceId, String pipelineName, ExecutionState state,
                                               FlowReason reason, long durationMs) {
        return new PipelineEvent(traceId, pipelineName, Type.PIPELINE_FINISH, null, null, 0, null,
                durationMs, null, state, reason);
    }

    public static PipelineEvent stageStart(String traceId, String pipelineName, String stageName,
                                           String loopName, int iteration) {
        return new PipelineEvent(traceId, pipelineName, Type.STAGE_START, stageName, loopName, iteration,
                null, -1L, null, null, null);
    }

    public static PipelineEvent stageFinish(String traceId, String pipelineName, String stageName,
                                            String loopName, int iteration, StageStatus status,
                                            long durationMs, String error) {
        FlowReason reason = error == null ? null : FlowReason.failed(error);
        return new PipelineEvent(traceId, pipelineName, Type.STAGE_FINISH, stageName, loopName, iteration,
                status, durationMs, null, null, reason);
    }

    public static PipelineEvent stateChanged(String traceId, String pipelineName, ExecutionState oldState,
                                             ExecutionState newState, FlowReason reason) {
        return new PipelineEvent(traceId, pipelineName, Type.STATE_CHANGED, null, null, 0, null,
                -1L, oldState, newState, reason);
    }

    public String traceId() {
        return traceId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    public Type type() {
        return type;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public String stageName() {
        return stageName;
    }

    public String loopName() {
        return loopName;
    }

    public int iteration() {
        return iteration;
    }

    public StageStatus status() {
        return status;
    }

    public long durationMs() {
        return durationMs;
    }

    public ExecutionState oldState() {
        return oldState;
    }

    public ExecutionState newState() {
        return newState;
    }

    public FlowReason reason() {
        return reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(timestamp).append("] ").append(type)
                .append(" trace=").append(traceId)
                .append(" pipeline=").append(pipelineName);
        if (stageName != null) {
            sb.append(" stage=").append(stageName);
        }
        if (loopName != null) {
            sb.append(" loop=").append(loopName).append('#').append(iteration);
        }
        if (status != null) {
            sb.append(" status=").append(status);
        }
        if (oldState != null || newState != null) {
            sb.append(" state=").append(oldState).append("->").append(newState);
        }
        if (reason != null) {
            sb.append(" reason=").append(reason);
        }
        if (durationMs >= 0) {
            sb.append(" durationMs=").append(durationMs);
        }
        return sb.toString();
    }
}
