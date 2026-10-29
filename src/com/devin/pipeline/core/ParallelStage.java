package com.devin.pipeline.core;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs its branches concurrently on the caller-provided executor, then lets the pipeline continue
 * after all of them joined.
 *
 * <p>Android production rule: parallel work must be explicitly scheduled. Branches share the same
 * {@link StageContext} by design, so they must not write the same attrs or payload fields
 * concurrently. A failing branch follows the pipeline error policy; abort/skip flags set by a
 * branch are honoured at the next stage boundary as usual.
 *
 * @param <C> payload type
 */
public final class ParallelStage<C> implements Stage<C> {

    private final String name;
    private final List<Stage<C>> branches;
    private final Executor executor;
    private final boolean failFast;

    public ParallelStage(String name, List<Stage<C>> branches, Executor executor) {
        this(name, branches, executor, false);
    }

    /**
     * @param failFast when true, a branch that stops (failure, abort, skip) cancels its siblings
     *                 instead of letting them run to completion
     */
    public ParallelStage(String name, List<Stage<C>> branches, Executor executor, boolean failFast) {
        if (branches == null || branches.isEmpty()) {
            throw new IllegalArgumentException("parallel stage '" + name + "' needs at least one branch");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.branches = List.copyOf(branches);
        this.executor = Objects.requireNonNull(executor,
                "executor is required for Android-safe parallel execution; provide an explicit executor");
        this.failFast = failFast;
    }

    @Override
    public String name() {
        return name;
    }

    public List<Stage<C>> branches() {
        return branches;
    }

    /** The executor chosen for Android-safe background scheduling. */
    public Executor executor() {
        return executor;
    }

    /** Whether a stopping branch cancels its siblings. */
    public boolean failFast() {
        return failFast;
    }

    /** Fallback used when such a stage is driven without the engine: sequential. */
    @Override
    public void handle(StageContext<C> ctx) throws Exception {
        for (Stage<C> branch : branches) {
            if (ctx.stopRequested()) {
                return;
            }
            if (branch.accept(ctx)) {
                branch.handle(ctx);
            }
        }
    }

    @Override
    public String toString() {
        return "ParallelStage[" + name + ", branches=" + branches.size() + "]";
    }
}
