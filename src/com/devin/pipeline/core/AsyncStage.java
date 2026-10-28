package com.devin.pipeline.core;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * A stage whose work completes outside the calling thread: the engine joins the returned future
 * before moving on, so ordering semantics stay identical to a synchronous stage.
 *
 * @param <C> payload type
 */
public interface AsyncStage<C> extends Stage<C> {

    CompletableFuture<Void> handleAsync(StageContext<C> ctx);

    /** Fallback used when such a stage is driven without the engine. */
    @Override
    default void handle(StageContext<C> ctx) throws Exception {
        try {
            handleAsync(ctx).join();
        } catch (CompletionException | CancellationException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error error) {
                throw error;
            }
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw e;
        }
    }

    static <C> AsyncStage<C> of(String name, Function<StageContext<C>, CompletableFuture<Void>> handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handler, "handler");
        return new AsyncStage<C>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public CompletableFuture<Void> handleAsync(StageContext<C> ctx) {
                return handler.apply(ctx);
            }

            @Override
            public String toString() {
                return "AsyncStage[" + name + "]";
            }
        };
    }
}
