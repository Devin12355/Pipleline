package com.devin.pipeline.exporter;

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
 * Writes {@code trace-<id>.json} into the configured directory for machines and log platforms.
 *
 * <p>The directory is mandatory: a relative path such as {@code out/traces} depends on the process
 * working directory, which does not exist in an Android app (files must live under
 * {@code context.getFilesDir()}, {@code getCacheDir()} or a scoped storage location).
 *
 * <p>Export failures go to an injectable sink - {@code System.err} is invisible on Android, so pass
 * {@code message -> Log.w("Pipeline", message)} there.
 */
public final class JsonTraceExporter implements TraceExporter {

    private final Path directory;
    private final Consumer<String> logger;

    /**
     * Fails by design: there is no reliable default directory across JVM and Android.
     *
     * @deprecated use {@link #JsonTraceExporter(Path)} with an explicit, platform specific
     *             directory (for example {@code context.getFilesDir().toPath()}).
     */
    @Deprecated
    public JsonTraceExporter() {
        throw new IllegalStateException("JsonTraceExporter needs an explicit directory: "
                + "new JsonTraceExporter(Path.of(\"out\", \"traces\")) on the JVM, "
                + "or new JsonTraceExporter(context.getFilesDir().toPath()) on Android.");
    }

    public JsonTraceExporter(Path directory) {
        this(directory, System.err::println);
    }

    /** @param logger where export failures are reported instead of {@code System.err}. */
    public JsonTraceExporter(Path directory, Consumer<String> logger) {
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
            Path file = directory.resolve("trace-" + trace.traceId() + ".json");
            Files.writeString(file, toJson(trace), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            logger.accept("[JsonTraceExporter] export failed for " + trace.traceId() + ": " + e);
        }
    }

    private static String toJson(ExecutionTrace trace) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\n");
        sb.append("  \"traceId\": ").append(str(trace.traceId())).append(",\n");
        sb.append("  \"pipeline\": ").append(str(trace.pipelineName())).append(",\n");
        sb.append("  \"startTime\": ").append(str(String.valueOf(trace.startTime()))).append(",\n");
        sb.append("  \"endTime\": ").append(str(String.valueOf(trace.endTime()))).append(",\n");
        sb.append("  \"durationMs\": ").append(trace.durationMs()).append(",\n");
        sb.append("  \"truncated\": ").append(trace.truncated()).append(",\n");
        sb.append("  \"reason\": {\n");
        sb.append("    \"type\": ").append(str(trace.reason() == null ? null
                : String.valueOf(trace.reason().type()))).append(",\n");
        sb.append("    \"code\": ").append(str(trace.reason() == null ? null : trace.reason().code())).append(",\n");
        sb.append("    \"message\": ").append(str(trace.reason() == null ? null : trace.reason().message())).append("\n");
        sb.append("  },\n");
        sb.append("  \"stages\": [\n");
        List<StageRecord> records = trace.records();
        for (int i = 0; i < records.size(); i++) {
            StageRecord r = records.get(i);
            sb.append("    {\"index\": ").append(r.index())
                    .append(", \"stage\": ").append(str(r.stageName()))
                    .append(", \"loop\": ").append(str(r.loopName()))
                    .append(", \"iteration\": ").append(r.iteration())
                    .append(", \"status\": ").append(str(String.valueOf(r.status())))
                    .append(", \"startTime\": ").append(str(String.valueOf(r.startTime())))
                    .append(", \"endTime\": ").append(str(String.valueOf(r.endTime())))
                    .append(", \"durationMs\": ").append(r.durationMs())
                    .append(", \"attempts\": ").append(r.attempts())
                    .append(", \"error\": ").append(str(r.error()))
                    .append('}')
                    .append(i == records.size() - 1 ? "\n" : ",\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static String str(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
