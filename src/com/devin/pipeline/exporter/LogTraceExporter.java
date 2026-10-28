package com.devin.pipeline.exporter;

import com.devin.pipeline.core.execution.StageStatus;
import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.core.trace.StageRecord;
import com.devin.pipeline.core.trace.TraceExporter;

import java.io.PrintStream;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Default exporter: one summary line plus one line per stage, written to a print stream or to a
 * line sink.
 *
 * <p>On Android prefer {@link #LogTraceExporter(java.util.function.Consumer)}:
 * {@code new LogTraceExporter(line -> Log.d("Pipeline", line))} - {@code System.out} is discarded
 * on a device.
 */
public final class LogTraceExporter implements TraceExporter {

    private final PrintStream out;
    private final Consumer<String> lineSink;

    public LogTraceExporter() {
        this(System.out);
    }

    public LogTraceExporter(PrintStream out) {
        this.out = out;
        this.lineSink = null;
    }

    /** Writes every line to the given sink, typically an Android logcat call. */
    public LogTraceExporter(Consumer<String> lineSink) {
        this.out = null;
        this.lineSink = Objects.requireNonNull(lineSink, "lineSink");
    }

    @Override
    public void export(ExecutionTrace trace) {
        if (trace == null) {
            return;
        }
        try {
            int success = 0;
            int skipped = 0;
            int failed = 0;
            int cancelled = 0;
            for (StageRecord record : trace.records()) {
                if (record.status() == StageStatus.SUCCESS) {
                    success++;
                } else if (record.status() == StageStatus.SKIPPED) {
                    skipped++;
                } else if (record.status() == StageStatus.FAILED) {
                    failed++;
                } else {
                    cancelled++;
                }
            }
            line("[trace] id=" + trace.traceId()
                    + " pipeline=" + trace.pipelineName()
                    + " reason=" + trace.reason()
                    + " durationMs=" + trace.durationMs()
                    + " truncated=" + trace.truncated()
                    + " stages=" + trace.records().size()
                    + " (success=" + success + " skipped=" + skipped
                    + " failed=" + failed + " cancelled=" + cancelled + ")");
            for (StageRecord record : trace.records()) {
                line("[trace]   " + record);
            }
        } catch (RuntimeException ignored) {
            // diagnostics must never break the execution that produced them
        }
    }

    private void line(String message) {
        if (lineSink != null) {
            lineSink.accept(message);
        } else if (out != null) {
            out.println(message);
        }
    }
}
