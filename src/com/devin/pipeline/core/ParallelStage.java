package com.devin.pipeline.core;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Runs its branches concurrently, then lets the pipeline continue after all of them joined.
 *
 * <p>This deliberately breaks the "one execution = one thread" premise, so it is opt-in:
 * branches share the same {@link StageContext} and must not write the same attrs or payload
 * fields concurrently. A failing branch follows the pipeline error policy; abort/skip flags
 * set by a branch are honoured at the next stage boundary as usual.
 *
 * @param <C> payload type
 */
public final class ParallelStage<C> implements Stage<C> {

    private final String name;
    private final List<Stage<C>> branches;
    private final Executor executor;

    public ParallelStage(String name, List<Stage<C>> branches, Executor executor) {
        if (branches == null || branches.isEmpty()) {
            throw new IllegalArgumentException("parallel stage '" + name + "' needs at least one branch");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.branches = List.copyOf(branches);
        this.executor = executor;
    }

    @Override
    public String name() {
        return name;
    }

    public List<Stage<C>> branches() {
        return branches;
    }

    /** {@code null} means the engine default ({@code ForkJoinPool.commonPool()}). */
    public Executor executor() {
        return executor;
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
