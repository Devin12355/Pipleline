package com.devin.pipeline.core.execution;

/** Outcome of a single stage execution inside one trace. */
public enum StageStatus {

    /** {@code accept(ctx)} returned false, or the stage was never reached. */
    SKIPPED,

    SUCCESS,

    FAILED,

    CANCELLED
}
