package com.devin.pipeline.exporter;

import com.devin.pipeline.core.execution.StageStatus;
import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.core.trace.StageRecord;
import com.devin.pipeline.core.trace.TraceExporter;

import java.io.PrintStream;

/**
 * Default exporter: one summary line plus one line per stage, written to a print stream.
 */
public final class LogTraceExporter implements TraceExporter {

    private final PrintStream out;

    public LogTraceExporter() {
        this(System.out);
    }

    public LogTraceExporter(PrintStream out) {
        this.out = out;
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
            out.println("[trace] id=" + trace.traceId()
                    + " pipeline=" + trace.pipelineName()
                    + " reason=" + trace.reason()
                    + " durationMs=" + trace.durationMs()
                    + " truncated=" + trace.truncated()
                    + " stages=" + trace.records().size()
                    + " (success=" + success + " skipped=" + skipped
                    + " failed=" + failed + " cancelled=" + cancelled + ")");
            for (StageRecord record : trace.records()) {
                out.println("[trace]   " + record);
            }
        } catch (RuntimeException ignored) {
            // diagnostics must never break the execution that produced them
        }
    }
}
