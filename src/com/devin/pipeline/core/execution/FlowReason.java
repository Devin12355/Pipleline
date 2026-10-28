package com.devin.pipeline.core.execution;

import java.util.Objects;

/**
 * Unified exit reason: {@code type + code + message}.
 *
 * <p>State answers "how did it end", reason answers "why", which is what troubleshooting,
 * auditing and monitoring need.
 */
public final class FlowReason {

    public enum Type {
        NORMAL,
        ABORTED,
        FAILED,
        CANCELLED,
        TIMEOUT,
        LOOP_TRUNCATED,
        REJECTED
    }

    private final Type type;
    private final String code;
    private final String message;

    private FlowReason(Type type, String code, String message) {
        this.type = type;
        this.code = code;
        this.message = message;
    }

    public static FlowReason normal() {
        return new FlowReason(Type.NORMAL, "NORMAL", "completed");
    }

    public static FlowReason normal(String message) {
        return new FlowReason(Type.NORMAL, "NORMAL", message);
    }

    public static FlowReason aborted(String message) {
        return new FlowReason(Type.ABORTED, "ABORTED", message);
    }

    public static FlowReason failed(String message) {
        return new FlowReason(Type.FAILED, "FAILED", message);
    }

    public static FlowReason failed(String stageName, Throwable error) {
        String detail = error.getMessage() == null ? error.getClass().getSimpleName()
                : error.getClass().getSimpleName() + ": " + error.getMessage();
        return new FlowReason(Type.FAILED, "STAGE_ERROR",
                (stageName == null ? "stage" : stageName) + " failed: " + detail);
    }

    public static FlowReason cancelled(String message) {
        return new FlowReason(Type.CANCELLED, "CANCELLED", message);
    }

    public static FlowReason timeout(long timeoutMs) {
        return new FlowReason(Type.TIMEOUT, "TIMEOUT", "execution exceeded " + timeoutMs + "ms");
    }

    public static FlowReason loopTruncated(int maxIterations) {
        return new FlowReason(Type.LOOP_TRUNCATED, "LOOP_TRUNCATED",
                "maxIterations bound reached: " + maxIterations);
    }

    public static FlowReason rejected(String message) {
        return new FlowReason(Type.REJECTED, "REJECTED", message);
    }

    public static FlowReason of(Type type, String code, String message) {
        return new FlowReason(Objects.requireNonNull(type, "type"), code, message);
    }

    public Type type() {
        return type;
    }

    public String code() {
        return code;
    }

    public String message() {
        return message;
    }

    public boolean isOk() {
        return type == Type.NORMAL || type == Type.LOOP_TRUNCATED;
    }

    @Override
    public String toString() {
        return type + (code == null ? "" : "[" + code + "]") + (message == null ? "" : ": " + message);
    }
}
