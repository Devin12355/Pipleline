package com.devin.pipeline.builder;

import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.StageContext;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.control.ErrorHandler;
import com.devin.pipeline.core.control.Interceptor;
import com.devin.pipeline.core.event.PipelineListener;
import com.devin.pipeline.core.trace.TraceExporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Fluent assembly of an immutable {@link Pipeline}.
 *
 * @param <C> payload type
 */
public final class PipelineBuilder<C> {

    /** Builder for one loop; consumed by {@link #loop(Consumer)}. */
    public static final class LoopBuilder<C> {

        private final List<Stage<C>> body = new ArrayList<>();
        private String name;
        private Predicate<StageContext<C>> condition = ctx -> true;
        private int maxIterations = Integer.MAX_VALUE;
        private ErrorAction iterationErrorPolicy;

        private LoopBuilder() {
        }

        public LoopBuilder<C> name(String name) {
            this.name = name;
            return this;
        }

        @SafeVarargs
        public final LoopBuilder<C> stages(Stage<C>... body) {
            return stages(Arrays.asList(body));
        }

        public LoopBuilder<C> stages(List<Stage<C>> body) {
            for (Stage<C> stage : body) {
                addStage(stage);
            }
            return this;
        }

        public LoopBuilder<C> addStage(Stage<C> stage) {
            body.add(Objects.requireNonNull(stage, "stage"));
            return this;
        }

        public LoopBuilder<C> addStage(String name, Consumer<StageContext<C>> handler) {
            return addStage(Stage.of(name, handler));
        }

        /** Checked before every iteration; a cooperative stop point. */
        public LoopBuilder<C> while_(Predicate<StageContext<C>> condition) {
            this.condition = Objects.requireNonNull(condition, "condition");
            return this;
        }

        /** Safety bound; reaching it marks the trace truncated and ends the loop normally. */
        public LoopBuilder<C> maxIterations(int maxIterations) {
            this.maxIterations = maxIterations;
            return this;
        }

        /** Error policy inside the loop; overrides the pipeline policy. */
        public LoopBuilder<C> onIterationError(ErrorAction action) {
            this.iterationErrorPolicy = Objects.requireNonNull(action, "action");
            return this;
        }

        private Pipeline.LoopStage<C> toStage(String defaultName) {
            String loopName = (name == null || name.isBlank()) ? defaultName : name;
            return new Pipeline.LoopStage<>(loopName, body, condition, maxIterations, iterationErrorPolicy);
        }
    }

    private final String name;
    private final List<Stage<C>> stages = new ArrayList<>();
    private final List<Stage<C>> finallyStages = new ArrayList<>();
    private final List<Interceptor<C>> interceptors = new ArrayList<>();
    private final List<PipelineListener<C>> listeners = new ArrayList<>();
    private final List<TraceExporter> exporters = new ArrayList<>();
    private ErrorHandler<C> errorHandler = ErrorHandler.abortAll();
    private long timeoutMs;
    private boolean traceEnabled = true;
    private int loopCount;

    private PipelineBuilder(String name) {
        this.name = name;
    }

    public static <C> PipelineBuilder<C> of(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("pipeline name must not be blank");
        }
        return new PipelineBuilder<>(name);
    }

    public PipelineBuilder<C> addStage(Stage<C> stage) {
        stages.add(Objects.requireNonNull(stage, "stage"));
        return this;
    }

    public PipelineBuilder<C> addStage(String name, Consumer<StageContext<C>> handler) {
        return addStage(Stage.of(name, handler));
    }

    @SafeVarargs
    public final PipelineBuilder<C> addStages(Stage<C>... stages) {
        return addStages(Arrays.asList(stages));
    }

    public PipelineBuilder<C> addStages(List<Stage<C>> stages) {
        for (Stage<C> stage : stages) {
            addStage(stage);
        }
        return this;
    }

    /** Registers a loop; its body is a sequence of normal stages executed per iteration. */
    public PipelineBuilder<C> loop(Consumer<LoopBuilder<C>> config) {
        Objects.requireNonNull(config, "config");
        LoopBuilder<C> loop = new LoopBuilder<>();
        config.accept(loop);
        loopCount++;
        stages.add(loop.toStage("loop-" + loopCount));
        return this;
    }

    /** Cleanup stage running on every exit path, including abort, cancel and timeout. */
    public PipelineBuilder<C> finallyStage(Stage<C> stage) {
        finallyStages.add(Objects.requireNonNull(stage, "stage"));
        return this;
    }

    public PipelineBuilder<C> finallyStage(String name, Consumer<StageContext<C>> handler) {
        return finallyStage(Stage.of(name, handler));
    }

    public PipelineBuilder<C> errorHandler(ErrorHandler<C> handler) {
        this.errorHandler = Objects.requireNonNull(handler, "handler");
        return this;
    }

    /** Per-execution deadline checked at stage boundaries; 0 disables it. */
    public PipelineBuilder<C> timeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public PipelineBuilder<C> traceEnabled(boolean enabled) {
        this.traceEnabled = enabled;
        return this;
    }

    public PipelineBuilder<C> traceDisabled() {
        return traceEnabled(false);
    }

    public PipelineBuilder<C> addInterceptor(Interceptor<C> interceptor) {
        interceptors.add(Objects.requireNonNull(interceptor, "interceptor"));
        return this;
    }

    public PipelineBuilder<C> addListener(PipelineListener<C> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return this;
    }

    public PipelineBuilder<C> addExporter(TraceExporter exporter) {
        exporters.add(Objects.requireNonNull(exporter, "exporter"));
        return this;
    }

    public Pipeline<C> build() {
        return new Pipeline<>(name, stages, finallyStages, interceptors, errorHandler, exporters,
                listeners, timeoutMs, traceEnabled);
    }
}
