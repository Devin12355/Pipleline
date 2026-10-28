package com.devin.pipeline.core;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The smallest processing unit of a pipeline: one stage does one thing.
 *
 * <p>This is the only interface business code must implement. Stages receive the shared
 * {@link StageContext}, mutate the payload in place and may influence flow through
 * {@code ctx.abort()} / {@code ctx.skipRest()} / {@code ctx.exitLoop()}.
 *
 * @param <C> payload type flowing through the pipeline
 */
public interface Stage<C> {

    /** Stable display name, must be unique enough to read in traces. */
    String name();

    /**
     * Conditional routing. {@code false} means the stage is {@code SKIPPED}, which is a normal
     * outcome and never an error.
     */
    default boolean accept(StageContext<C> ctx) {
        return true;
    }

    /** Business logic. Throwing signals a real failure and routes to the error policy. */
    void handle(StageContext<C> ctx) throws Exception;

    /** Lambda friendly factory: always runs. */
    static <C> Stage<C> of(String name, Consumer<StageContext<C>> handler) {
        return of(name, null, handler);
    }

    /** Lambda friendly factory running only when {@code condition} accepts the context. */
    static <C> Stage<C> of(String name,
                           Predicate<StageContext<C>> condition,
                           Consumer<StageContext<C>> handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handler, "handler");
        Predicate<StageContext<C>> guard = condition == null ? ctx -> true : condition;
        return new Stage<C>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean accept(StageContext<C> ctx) {
                return guard.test(ctx);
            }

            @Override
            public void handle(StageContext<C> ctx) {
                handler.accept(ctx);
            }

            @Override
            public String toString() {
                return "Stage[" + name + "]";
            }
        };
    }
}
