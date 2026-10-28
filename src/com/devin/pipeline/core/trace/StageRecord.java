package com.devin.pipeline.core.trace;

import com.devin.pipeline.core.execution.StageStatus;

import java.time.Instant;

/** One row of a trace: a single stage execution of a single iteration. */
public final class StageRecord {

    private final int index;
    private final String stageName;
    private final String loopName;
    private final int iteration;
    private final StageStatus status;
    private final Instant startTime;
    private final Instant endTime;
    private final long durationMs;
    private final String error;

    public StageRecord(int index,
                       String stageName,
                       String loopName,
                       int iteration,
                       StageStatus status,
                       Instant startTime,
                       Instant endTime,
                       String error) {
        this.index = index;
        this.stageName = stageName;
        this.loopName = loopName;
        this.iteration = iteration;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.durationMs = (startTime == null || endTime == null) ? -1L
                : endTime.toEpochMilli() - startTime.toEpochMilli();
        this.error = error;
    }

    public static StageRecord success(int index, String stageName, String loopName, int iteration,
                                      Instant start, Instant end) {
        return new StageRecord(index, stageName, loopName, iteration, StageStatus.SUCCESS, start, end, null);
    }

    public static StageRecord skipped(int index, String stageName, String loopName, int iteration,
                                      Instant start, Instant end) {
        return new StageRecord(index, stageName, loopName, iteration, StageStatus.SKIPPED, start, end, null);
    }

    public static StageRecord cancelled(int index, String stageName, String loopName, int iteration,
                                        Instant start, Instant end) {
        return new StageRecord(index, stageName, loopName, iteration, StageStatus.CANCELLED, start, end, null);
    }

    public static StageRecord failed(int index, String stageName, String loopName, int iteration,
                                     Instant start, Instant end, Throwable error) {
        return new StageRecord(index, stageName, loopName, iteration, StageStatus.FAILED, start, end,
                describe(error));
    }

    public static StageRecord failed(int index, String stageName, String loopName, int iteration,
                                     Instant start, Instant end, String error) {
        return new StageRecord(index, stageName, loopName, iteration, StageStatus.FAILED, start, end, error);
    }

    private static String describe(Throwable error) {
        if (error == null) {
            return null;
        }
        String message = error.getMessage();
        return message == null ? error.getClass().getName() : error.getClass().getName() + ": " + message;
    }

    /** Global order inside the execution, 0-based. */
    public int index() {
        return index;
    }

    public String stageName() {
        return stageName;
    }

    /** Name of the enclosing loop, {@code null} when the stage is not part of a loop. */
    public String loopName() {
        return loopName;
    }

    /** Iteration of the enclosing loop, 0 when there is no enclosing loop. */
    public int iteration() {
        return iteration;
    }

    public StageStatus status() {
        return status;
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

    public String error() {
        return error;
    }

    public boolean inLoop() {
        return loopName != null;
    }

    @Override
    public String toString() {
        return "#" + index + " " + stageName
                + (loopName == null ? "" : " (" + loopName + "#" + iteration + ")")
                + " " + status + " " + durationMs + "ms"
                + (error == null ? "" : " error=" + error);
    }
}
