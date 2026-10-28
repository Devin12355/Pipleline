package com.devin.pipeline.dsl;

import com.devin.pipeline.builder.PipelineBuilder;
import com.devin.pipeline.core.ParallelStage;
import com.devin.pipeline.core.Pipeline;
import com.devin.pipeline.core.Stage;
import com.devin.pipeline.core.control.ErrorAction;
import com.devin.pipeline.core.control.ErrorHandler;
import com.devin.pipeline.core.control.RetryPolicy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Loads a {@link Pipeline} from a YAML document.
 *
 * <p>YAML declares structure and numbers (names, order, loops, parallel groups, retries,
 * timeout); behaviour stays in Java and is bound by id through a {@link StageRegistry}.
 *
 * <p>Supported keys:
 * <pre>
 * name, timeoutMs, trace, errorAction (ABORT | CONTINUE | registered id), retry (pipeline default)
 * stages:
 *   - name, handler, when, retry, finally: true
 *   - name, loop: { while, maxIterations, onIterationError, stages }
 *   - name, subPipeline: &lt;registered pipeline id&gt;
 *   - name, parallel: { executor?, stages }
 * </pre>
 */
public final class YamlPipelineLoader {

    private YamlPipelineLoader() {
    }

    public static <C> Pipeline<C> load(Path file, StageRegistry<C> registry) {
        String yaml;
        try {
            yaml = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("cannot read pipeline file: " + file, e);
        }
        return load(yaml, registry);
    }

    @SuppressWarnings("unchecked")
    public static <C> Pipeline<C> load(String yaml, StageRegistry<C> registry) {
        Object document = MiniYaml.parse(yaml);
        if (!(document instanceof Map)) {
            throw new IllegalArgumentException("pipeline document must be a mapping at top level");
        }
        return build((Map<String, Object>) document, registry);
    }

    @SuppressWarnings("unchecked")
    private static <C> Pipeline<C> build(Map<String, Object> root, StageRegistry<C> registry) {
        String name = text(root.get("name"), "pipeline");
        PipelineBuilder<C> builder = PipelineBuilder.of(name);

        Object timeout = root.get("timeoutMs");
        if (timeout instanceof Number number) {
            builder.timeout(number.longValue());
        }
        Object trace = root.get("trace");
        if (trace instanceof Boolean enabled) {
            builder.traceEnabled(enabled);
        }
        Object errorAction = root.get("errorAction");
        if (errorAction != null) {
            builder.errorHandler(errorHandler(String.valueOf(errorAction), registry));
        }
        Object retry = root.get("retry");
        if (retry instanceof Map<?, ?> retryMap) {
            builder.retryPolicy(retryPolicy((Map<String, Object>) retryMap));
        }

        Object stages = root.get("stages");
        if (stages instanceof List<?> list) {
            for (Object item : list) {
                if (!(item instanceof Map<?, ?> itemMap)) {
                    throw new IllegalArgumentException("each entry of 'stages' must be a mapping");
                }
                Map<String, Object> stageMap = (Map<String, Object>) itemMap;
                Stage<C> stage = buildStage(stageMap, registry);
                if (truthy(stageMap.get("finally"))) {
                    builder.finallyStage(stage);
                } else {
                    builder.addStage(stage);
                }
            }
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <C> Stage<C> buildStage(Map<String, Object> item, StageRegistry<C> registry) {
        String name = text(item.get("name"), null);
        if (name == null) {
            throw new IllegalArgumentException("every stage needs a 'name': " + item);
        }
        boolean isFinally = truthy(item.get("finally"));

        Stage<C> stage;
        Object loop = item.get("loop");
        Object subPipeline = item.get("subPipeline");
        Object parallel = item.get("parallel");

        if (isFinally && (loop != null || subPipeline != null || parallel != null)) {
            throw new IllegalArgumentException("finally stage '" + name + "' only supports handler");
        }

        if (loop instanceof Map<?, ?> loopMap) {
            stage = buildLoop(name, (Map<String, Object>) loopMap, registry);
        } else if (subPipeline != null) {
            stage = new Pipeline.SubPipelineStage<>(name, registry.pipeline(String.valueOf(subPipeline)));
        } else if (parallel instanceof Map<?, ?> parallelMap) {
            stage = buildParallel(name, (Map<String, Object>) parallelMap, registry);
        } else {
            String handlerId = text(item.get("handler"), null);
            if (handlerId == null) {
                throw new IllegalArgumentException("stage '" + name
                        + "' needs one of handler/loop/subPipeline/parallel");
            }
            String whenId = text(item.get("when"), null);
            stage = Stage.of(name,
                    whenId == null ? null : registry.condition(whenId),
                    registry.handler(handlerId));
        }

        Object retry = item.get("retry");
        if (retry instanceof Map<?, ?> retryMap) {
            if (loop != null || subPipeline != null || parallel != null) {
                throw new IllegalArgumentException("retry is not supported on loop/subPipeline/parallel stage '"
                        + name + "'; configure it on their inner stages instead");
            }
            stage = Stage.withRetry(stage, retryPolicy((Map<String, Object>) retryMap));
        }
        return stage;
    }

    @SuppressWarnings("unchecked")
    private static <C> Pipeline.LoopStage<C> buildLoop(String name, Map<String, Object> loopMap,
                                                       StageRegistry<C> registry) {
        String whileId = text(loopMap.get("while"), null);
        int maxIterations = intVal(loopMap.get("maxIterations"), Integer.MAX_VALUE);
        ErrorAction onIterationError = null;
        Object errorAction = loopMap.get("onIterationError");
        if (errorAction != null) {
            onIterationError = ErrorAction.valueOf(String.valueOf(errorAction).toUpperCase());
        }
        List<Stage<C>> body = buildStageList(loopMap.get("stages"), registry, "loop '" + name + "'");
        return new Pipeline.LoopStage<>(name, body,
                whileId == null ? null : registry.condition(whileId), maxIterations, onIterationError);
    }

    @SuppressWarnings("unchecked")
    private static <C> ParallelStage<C> buildParallel(String name, Map<String, Object> parallelMap,
                                                      StageRegistry<C> registry) {
        List<Stage<C>> branches = buildStageList(parallelMap.get("stages"), registry,
                "parallel '" + name + "'");
        Executor executor = null;
        Object executorId = parallelMap.get("executor");
        if (executorId != null) {
            executor = registry.executor(String.valueOf(executorId));
        }
        return new ParallelStage<>(name, branches, executor);
    }

    @SuppressWarnings("unchecked")
    private static <C> List<Stage<C>> buildStageList(Object stages, StageRegistry<C> registry, String where) {
        if (!(stages instanceof List<?> list) || list.isEmpty()) {
            throw new IllegalArgumentException(where + " needs at least one stage");
        }
        List<Stage<C>> result = new java.util.ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> itemMap)) {
                throw new IllegalArgumentException("every entry of " + where + ".stages must be a mapping");
            }
            Map<String, Object> stageMap = (Map<String, Object>) itemMap;
            if (truthy(stageMap.get("finally"))) {
                throw new IllegalArgumentException("finally is not allowed inside " + where);
            }
            result.add(buildStage(stageMap, registry));
        }
        return result;
    }

    private static <C> ErrorHandler<C> errorHandler(String id, StageRegistry<C> registry) {
        switch (id.toUpperCase()) {
            case "ABORT":
                return ErrorHandler.abortAll();
            case "CONTINUE":
                return ErrorHandler.continueAll();
            default:
                return registry.errorHandler(id);
        }
    }

    private static RetryPolicy retryPolicy(Map<String, Object> map) {
        RetryPolicy policy = RetryPolicy.of(intVal(map.get("maxAttempts"), 1));
        Object initialDelay = map.get("initialDelayMs");
        if (initialDelay instanceof Number number) {
            policy = policy.initialDelayMs(number.longValue());
        }
        Object multiplier = map.get("multiplier");
        if (multiplier instanceof Number number) {
            policy = policy.multiplier(number.doubleValue());
        }
        Object maxDelay = map.get("maxDelayMs");
        if (maxDelay instanceof Number number) {
            policy = policy.maxDelayMs(number.longValue());
        }
        return policy;
    }

    private static boolean truthy(Object value) {
        return Boolean.TRUE.equals(value);
    }

    private static int intVal(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private static String text(Object value, String defaultValue) {
        return value == null ? defaultValue : String.valueOf(value);
    }
}
