package com.devin.pipeline.core.control;

import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.StageContext;

/**
 * Failure policy consulted by the engine after a stage threw.
 *
 * @param <C> payload type
 */
public interface ErrorHandler<C> {

    ErrorAction onError(Stage<C> stage, StageContext<C> ctx, Throwable error);

    /** Default policy: abort on any failure. */
    static <C> ErrorHandler<C> abortAll() {
        return (stage, ctx, error) -> ErrorAction.ABORT;
    }

    /** Policy: never let a failing stage stop the pipeline. */
    static <C> ErrorHandler<C> continueAll() {
        return (stage, ctx, error) -> ErrorAction.CONTINUE;
    }
}
