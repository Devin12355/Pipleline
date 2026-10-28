package com.devin.pipeline.core.execution;

/**
 * Lifecycle state of one execution. Terminal states are one-shot and irreversible.
 */
public enum ExecutionState {

    PENDING,
    RUNNING,
    COMPLETED,
    ABORTED,
    FAILED,
    CANCELLED,
    TIMEOUT;

    public boolean isTerminal() {
        return this == COMPLETED || this == ABORTED || this == FAILED
                || this == CANCELLED || this == TIMEOUT;
    }
}
