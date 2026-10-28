package com.devin.pipeline.core.control;

/**
 * Cooperative cancellation flag shared between the pipeline execution and the outside world.
 *
 * <p>The token never kills a thread: the engine reads it at stage boundaries, so the currently
 * running stage always finishes first.
 */
public final class CancelToken {

    private volatile boolean cancelled;
    private volatile String reason;

    public boolean isCancelled() {
        return cancelled;
    }

    /** Reason supplied by whoever cancelled, may be {@code null}. */
    public String reason() {
        return reason;
    }

    public void cancel() {
        cancel("cancelled");
    }

    public void cancel(String reason) {
        if (reason != null) {
            this.reason = reason;
        }
        this.cancelled = true;
    }

    @Override
    public String toString() {
        return cancelled ? "CancelToken[cancelled, reason=" + reason + "]" : "CancelToken[active]";
    }
}
