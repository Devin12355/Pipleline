package com.devin.pipeline.core;

import com.devin.pipeline.core.control.CancelToken;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Data carrier and flow controller for one execution.
 *
 * <p>Exactly one context is created per {@code execute(...)} and the same reference travels
 * through every stage: the payload is mutated in place (zero copy) while {@code attrs} carry
 * auxiliary data. Because one execution is single-threaded, plain collections are enough.
 *
 * @param <C> payload type
 */
public final class StageContext<C> {

    /** Type safe attribute key; collect them in constant classes to form a data contract. */
    public static final class TypedKey<T> {

        private final String name;

        private TypedKey(String name) {
            this.name = name;
        }

        public static <T> TypedKey<T> of(String name) {
            return new TypedKey<>(Objects.requireNonNull(name, "name"));
        }

        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TypedKey<?> other && name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Coordinates of the loop currently being executed. */
    public static final class LoopInfo {

        public final String name;

        /** 1-based iteration counter; 0 means "not in a loop". */
        public final int iteration;

        public LoopInfo(String name, int iteration) {
            this.name = name;
            this.iteration = iteration;
        }

        @Override
        public String toString() {
            return name == null ? "loop:0" : name + "#" + iteration;
        }
    }

    private static final LoopInfo NO_LOOP = new LoopInfo(null, 0);

    private final String pipelineName;
    private final String traceId;
    private final CancelToken cancelToken;
    private final Map<String, Object> attrs = new HashMap<>();

    private C payload;
    private volatile LoopInfo currentLoop = NO_LOOP;
    private volatile String currentStage;
    private volatile boolean aborted;
    private volatile boolean skipRest;
    private volatile boolean exitLoop;
    private volatile String abortReason;

    public StageContext(String pipelineName, String traceId, C payload) {
        this(pipelineName, traceId, payload, new CancelToken());
    }

    public StageContext(String pipelineName, String traceId, C payload, CancelToken cancelToken) {
        this.pipelineName = pipelineName;
        this.traceId = traceId;
        this.payload = payload;
        this.cancelToken = cancelToken == null ? new CancelToken() : cancelToken;
    }

    // ---- data flow -------------------------------------------------------

    public C payload() {
        return payload;
    }

    public void setPayload(C payload) {
        this.payload = payload;
    }

    public StageContext<C> setAttr(String key, Object value) {
        attrs.put(key, value);
        return this;
    }

    public <V> StageContext<C> setAttr(TypedKey<V> key, V value) {
        attrs.put(key.name(), value);
        return this;
    }

    public boolean hasAttr(String key) {
        return attrs.containsKey(key);
    }

    @SuppressWarnings("unchecked")
    public <V> V getAttr(String key) {
        return (V) attrs.get(key);
    }

    public <V> V getAttr(String key, V defaultValue) {
        V value = getAttr(key);
        return value == null ? defaultValue : value;
    }

    @SuppressWarnings("unchecked")
    public <V> V getAttr(TypedKey<V> key) {
        return (V) attrs.get(key.name());
    }

    public <V> V getAttr(TypedKey<V> key, V defaultValue) {
        V value = getAttr(key);
        return value == null ? defaultValue : value;
    }

    public Object removeAttr(String key) {
        return attrs.remove(key);
    }

    public Map<String, Object> attrs() {
        return Collections.unmodifiableMap(new HashMap<>(attrs));
    }

    // ---- flow control ----------------------------------------------------

    /** Aborts the whole pipeline with the default reason (state {@code ABORTED}). */
    public void abort() {
        abort("aborted");
    }

    /** Aborts the whole pipeline with a business reason (state {@code ABORTED}). */
    public void abort(String reason) {
        this.abortReason = reason;
        this.aborted = true;
    }

    public boolean isAborted() {
        return aborted;
    }

    public String abortReason() {
        return abortReason;
    }

    /** Skips every remaining stage; the execution still ends as {@code COMPLETED}. */
    public void skipRest() {
        this.skipRest = true;
    }

    public boolean isSkipRest() {
        return skipRest;
    }

    /** Leaves the enclosing loop after the current iteration; the pipeline continues. */
    public void exitLoop() {
        this.exitLoop = true;
    }

    public boolean isExitLoop() {
        return exitLoop;
    }

    /** True when anything already asked the execution to stop. */
    public boolean stopRequested() {
        return aborted || skipRest || cancelToken.isCancelled();
    }

    // ---- runtime info ----------------------------------------------------

    public String pipelineName() {
        return pipelineName;
    }

    public String traceId() {
        return traceId;
    }

    /** Stage currently being executed, updated by the engine. */
    public String currentStage() {
        return currentStage;
    }

    /** Coordinates of the enclosing loop; {@code iteration == 0} when not in a loop. */
    public LoopInfo currentLoop() {
        return currentLoop;
    }

    public CancelToken cancelToken() {
        return cancelToken;
    }

    void enterLoop(String loopName, int iteration) {
        this.currentLoop = new LoopInfo(loopName, iteration);
    }

    void restoreLoop(LoopInfo previous) {
        this.currentLoop = previous == null ? NO_LOOP : previous;
    }

    void setCurrentStage(String stageName) {
        this.currentStage = stageName;
    }

    void resetExitLoop() {
        this.exitLoop = false;
    }

    @Override
    public String toString() {
        return "StageContext{pipeline=" + pipelineName
                + ", trace=" + traceId
                + ", stage=" + currentStage
                + ", loop=" + currentLoop
                + ", aborted=" + aborted
                + ", skipRest=" + skipRest
                + ", attrKeys=" + attrs.keySet()
                + '}';
    }
}
