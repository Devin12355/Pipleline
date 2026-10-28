package com.devin.pipeline.core.control;

import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.StageContext;

/**
 * Around advice applied to every stage.
 *
 * <p>Registration order is execution order: {@code beforeStage} runs in order and
 * {@code afterStage} in reverse, so interceptors nest properly. All methods are optional and
 * exceptions thrown by an interceptor never change the outcome of the execution.
 *
 * <p><b>Per attempt semantics</b> with a retry policy: {@code beforeStage} / {@code afterStage}
 * are called once per attempt and always as a pair - a failed attempt still gets its
 * {@code afterStage} before {@code onStageError}. Count attempts with
 * {@link com.devin.pipeline.core.trace.StageRecord#attempts()} rather than counting calls.
 *
 * @param <C> payload type
 */
public interface Interceptor<C> {

    default void beforeStage(Stage<C> stage, StageContext<C> ctx) {
    }

    default void afterStage(Stage<C> stage, StageContext<C> ctx) {
    }

    /** Invoked before {@code ErrorHandler}, so every failure is visible. */
    default void onStageError(Stage<C> stage, StageContext<C> ctx, Throwable error) {
    }
}
