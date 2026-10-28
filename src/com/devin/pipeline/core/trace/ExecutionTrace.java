package com.devin.pipeline.core.trace;

import com.devin.pipeline.core.execution.FlowReason;
import com.devin.pipeline.core.execution.StageStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One trace per execution: identity, timing, per-stage records and the terminal reason.
 *
 * <p>Mutated by the engine while the execution runs; exporters are invoked only after the trace
 * has been completed.
 */
public final class ExecutionTrace {

    private final String traceId;
    private final String pipelineName;
    private final Instant startTime;
    private final List<StageRecord> records = new ArrayList<>();
    private volatile Instant endTime;
    private volatile long durationMs = -1L;
    private volatile boolean truncated;
    private volatile FlowReason reason;

    public ExecutionTrace(String traceId, String pipelineName, Instant startTime) {
        this.traceId = traceId;
        this.pipelineName = pipelineName;
        this.startTime = startTime == null ? Instant.now() : startTime;
    }

    /** Appends a stage record; called by the engine in execution order. */
    public void add(StageRecord record) {
        if (record != null) {
            records.add(record);
        }
    }

    /** Marks the trace as not having ended naturally (e.g. maxIterations bound reached). */
    public void markTruncated() {
        this.truncated = true;
    }

    /** Completes the trace with the terminal reason. */
    public void complete(FlowReason reason, Instant endTime) {
        this.reason = reason;
        this.endTime = endTime == null ? Instant.now() : endTime;
        this.durationMs = this.endTime.toEpochMilli() - startTime.toEpochMilli();
    }

    public String traceId() {
        return traceId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    public Instant startTime() {
        return startTime;
    }

    public Instant endTime() {
        return endTime;
    }

    public long durationMs() {
        return durationMs;
    }

    public boolean truncated() {
        return truncated;
    }

    public FlowReason reason() {
        return reason;
    }

    public List<StageRecord> records() {
        return Collections.unmodifiableList(records);
    }

    public boolean hasFailure() {
        for (StageRecord record : records) {
            if (record.status() == StageStatus.FAILED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "ExecutionTrace{id=" + traceId
                + ", pipeline=" + pipelineName
                + ", durationMs=" + durationMs
                + ", truncated=" + truncated
                + ", reason=" + reason
                + ", stages=" + records.size()
                + '}';
    }
}
