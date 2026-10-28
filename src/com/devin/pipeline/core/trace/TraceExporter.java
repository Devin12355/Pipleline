package com.devin.pipeline.core.trace;

/** SPI for shipping a finished trace somewhere. */
public interface TraceExporter {

    /**
     * Called once per execution after the trace is complete.
     *
     * <p>Implementations must not let exceptions escape: export failures are diagnostics and
     * must never break the execution that produced them.
     */
    void export(ExecutionTrace trace);
}
