package com.devin.pipeline.core.control;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Retry policy for a stage: how many attempts, how long to wait between them, and which
 * failures deserve another try.
 *
 * <p>Immutable; the {@code with*} methods return a configured copy.
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final long initialDelayMs;
    private final double multiplier;
    private final long maxDelayMs;
    private final Predicate<Throwable> retryOn;

    private RetryPolicy(int maxAttempts, long initialDelayMs, double multiplier,
                        long maxDelayMs, Predicate<Throwable> retryOn) {
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.multiplier = multiplier;
        this.maxDelayMs = maxDelayMs;
        this.retryOn = retryOn;
    }

    /** {@code maxAttempts} total tries (1 = no retry). */
    public static RetryPolicy of(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1: " + maxAttempts);
        }
        return new RetryPolicy(maxAttempts, 0L, 1.0d, Long.MAX_VALUE, error -> true);
    }

    /** Delay before the first retry, milliseconds. */
    public RetryPolicy initialDelayMs(long initialDelayMs) {
        return new RetryPolicy(maxAttempts, Math.max(0L, initialDelayMs), multiplier,
                maxDelayMs, retryOn);
    }

    /** Backoff multiplier applied per failed attempt, {@code 1.0} = fixed delay. */
    public RetryPolicy multiplier(double multiplier) {
        return new RetryPolicy(maxAttempts, initialDelayMs,
                multiplier <= 0 ? 1.0d : multiplier, maxDelayMs, retryOn);
    }

    /** Upper bound for a single backoff delay, milliseconds. */
    public RetryPolicy maxDelayMs(long maxDelayMs) {
        return new RetryPolicy(maxAttempts, initialDelayMs, multiplier,
                Math.max(0L, maxDelayMs), retryOn);
    }

    /** Only failures matching this predicate are retried; default: all. */
    public RetryPolicy retryOn(Predicate<Throwable> retryOn) {
        return new RetryPolicy(maxAttempts, initialDelayMs, multiplier, maxDelayMs,
                Objects.requireNonNull(retryOn, "retryOn"));
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public boolean shouldRetry(int failedAttempt, Throwable error) {
        return failedAttempt < maxAttempts && retryOn.test(error);
    }

    /** Delay before the retry following {@code failedAttempt} (1-based). */
    public long delayForAttempt(int failedAttempt) {
        if (initialDelayMs <= 0L) {
            return 0L;
        }
        double delay = initialDelayMs * Math.pow(multiplier, Math.max(0, failedAttempt - 1));
        return (long) Math.min(Math.max(0.0d, delay), maxDelayMs);
    }

    @Override
    public String toString() {
        return "RetryPolicy{maxAttempts=" + maxAttempts
                + ", initialDelayMs=" + initialDelayMs
                + ", multiplier=" + multiplier
                + ", maxDelayMs=" + maxDelayMs + "}";
    }
}
