package com.devin.pipeline.dsl;

import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.StageContext;
import com.devin.pipeline.core.control.ErrorHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Binds the ids used in a YAML document to Java logic: YAML declares structure, code provides
 * behaviour. One registry instance belongs to one payload type.
 *
 * @param <C> payload type
 */
public final class StageRegistry<C> {

    private final Map<String, Consumer<StageContext<C>>> handlers = new HashMap<>();
    private final Map<String, Predicate<StageContext<C>>> conditions = new HashMap<>();
    private final Map<String, Pipeline<C>> pipelines = new HashMap<>();
    private final Map<String, Executor> executors = new HashMap<>();
    private final Map<String, ErrorHandler<C>> errorHandlers = new HashMap<>();

    /** Registers the handler of a stage (the stage body). */
    public StageRegistry<C> handler(String id, Consumer<StageContext<C>> handler) {
        handlers.put(id, Objects.requireNonNull(handler, "handler"));
        return this;
    }

    /** Registers a condition usable as {@code when:} / {@code while:}. */
    public StageRegistry<C> condition(String id, Predicate<StageContext<C>> condition) {
        conditions.put(id, Objects.requireNonNull(condition, "condition"));
        return this;
    }

    /** Registers a pipeline usable as {@code subPipeline:}. */
    public StageRegistry<C> pipeline(String id, Pipeline<C> pipeline) {
        pipelines.put(id, Objects.requireNonNull(pipeline, "pipeline"));
        return this;
    }

    /** Registers an executor usable as {@code parallel.executor:}. */
    public StageRegistry<C> executor(String id, Executor executor) {
        executors.put(id, Objects.requireNonNull(executor, "executor"));
        return this;
    }

    /** Registers a custom error handler usable as {@code errorAction: <id>}. */
    public StageRegistry<C> errorHandler(String id, ErrorHandler<C> errorHandler) {
        errorHandlers.put(id, Objects.requireNonNull(errorHandler, "errorHandler"));
        return this;
    }

    public Consumer<StageContext<C>> handler(String id) {
        Consumer<StageContext<C>> handler = handlers.get(id);
        if (handler == null) {
            throw new IllegalArgumentException("unknown handler id in YAML: '" + id + "'");
        }
        return handler;
    }

    public Predicate<StageContext<C>> condition(String id) {
        Predicate<StageContext<C>> condition = conditions.get(id);
        if (condition == null) {
            throw new IllegalArgumentException("unknown condition id in YAML: '" + id + "'");
        }
        return condition;
    }

    public Pipeline<C> pipeline(String id) {
        Pipeline<C> pipeline = pipelines.get(id);
        if (pipeline == null) {
            throw new IllegalArgumentException("unknown subPipeline id in YAML: '" + id + "'");
        }
        return pipeline;
    }

    public Executor executor(String id) {
        Executor executor = executors.get(id);
        if (executor == null) {
            throw new IllegalArgumentException("unknown executor id in YAML: '" + id + "'");
        }
        return executor;
    }

    public ErrorHandler<C> errorHandler(String id) {
        ErrorHandler<C> errorHandler = errorHandlers.get(id);
        if (errorHandler == null) {
            throw new IllegalArgumentException("unknown errorHandler id in YAML: '" + id + "'");
        }
        return errorHandler;
    }
}
