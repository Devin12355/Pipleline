package com.devin.pipeline.exporter;

import com.devin.pipeline.core.execution.StageStatus;
import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.core.trace.StageRecord;
import com.devin.pipeline.core.trace.TraceExporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Writes {@code trace-<id>.md}: a Mermaid flowchart coloured by stage status, for post-hoc review.
 *
 * <p>Like {@link JsonTraceExporter} the output directory is mandatory: a relative path is not
 * reliable on Android, where files must live under the app's own directories.
 */
public final class MermaidExporter implements TraceExporter {

    private final Path directory;
    private final Consumer<String> logger;

    /**
     * Fails by design: there is no reliable default directory across JVM and Android.
     *
     * @deprecated use {@link #MermaidExporter(Path)} with an explicit, platform specific directory.
     */
    @Deprecated
    public MermaidExporter() {
        throw new IllegalStateException("MermaidExporter needs an explicit directory: "
                + "new MermaidExporter(Path.of(\"out\", \"traces\")) on the JVM, "
                + "or new MermaidExporter(context.getFilesDir().toPath()) on Android.");
    }

    public MermaidExporter(Path directory) {
        this(directory, System.err::println);
    }

    /** @param logger where export failures are reported instead of {@code System.err}. */
    public MermaidExporter(Path directory, Consumer<String> logger) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.logger = logger == null ? message -> { } : logger;
    }

    @Override
    public void export(ExecutionTrace trace) {
        if (trace == null) {
            return;
        }
        try {
            Files.createDirectories(directory);
            Path file = directory.resolve("trace-" + trace.traceId() + ".md");
            Files.writeString(file, render(trace), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            logger.accept("[MermaidExporter] export failed for " + trace.traceId() + ": " + e);
        }
    }

    private static String render(ExecutionTrace trace) {
        List<StageRecord> records = trace.records();
        StringBuilder sb = new StringBuilder(512);
        sb.append("# Trace ").append(trace.traceId()).append("\n\n");
        sb.append("```mermaid\nflowchart LR\n");
        for (StageRecord r : records) {
            String label = r.stageName()
                    + (r.loopName() == null ? "" : "\\n" + r.loopName() + "#" + r.iteration())
                    + "\\n" + r.status() + " " + r.durationMs() + "ms";
            sb.append("  ").append(nodeId(r)).append('[').append('"').append(escape(label)).append('"').append("]\n");
            sb.append("  style ").append(nodeId(r)).append(" fill:").append(colorOf(r.status())).append("\n");
        }
        if (!records.isEmpty()) {
            sb.append("  start((start)) --> ").append(nodeId(records.get(0))).append("\n");
        }
        for (int i = 1; i < records.size(); i++) {
            sb.append("  ").append(nodeId(records.get(i - 1)))
                    .append(" --> ")
                    .append(nodeId(records.get(i)))
                    .append("\n");
        }
        sb.append("```\n\n");
        sb.append("| # | stage | loop | iteration | status | durationMs | error |\n");
        sb.append("|---|-------|------|-----------|--------|------------|-------|\n");
        for (StageRecord r : records) {
            sb.append("| ").append(r.index())
                    .append(" | ").append(r.stageName())
                    .append(" | ").append(r.loopName() == null ? "-" : r.loopName())
                    .append(" | ").append(r.iteration())
                    .append(" | ").append(r.status())
                    .append(" | ").append(r.durationMs())
                    .append(" | ").append(r.error() == null ? "-" : escape(r.error()))
                    .append(" |\n");
        }
        sb.append("\nreason: ").append(trace.reason()).append('\n');
        sb.append("durationMs: ").append(trace.durationMs()).append('\n');
        sb.append("truncated: ").append(trace.truncated()).append('\n');
        return sb.toString();
    }

    private static String nodeId(StageRecord r) {
        return "s" + r.index();
    }

    private static String colorOf(StageStatus status) {
        if (status == StageStatus.SUCCESS) {
            return "#dcfce7";
        }
        if (status == StageStatus.SKIPPED) {
            return "#e5e7eb";
        }
        if (status == StageStatus.FAILED) {
            return "#fecaca";
        }
        return "#fed7aa";
    }

    private static String escape(String value) {
        return value.replace("\"", "'").replace("\n", " ");
    }
}
