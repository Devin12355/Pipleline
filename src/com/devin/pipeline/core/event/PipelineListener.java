package com.devin.pipeline.core.event;

/**
 * Callback receiving every structured event of an execution.
 *
 * <p>Listener exceptions are swallowed: listeners observe, they must never break an execution.
 *
 * @param <C> payload type
 */
public interface PipelineListener<C> {

    default void onEvent(PipelineEvent event) {
    }
}
