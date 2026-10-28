package com.devin.pipeline.exporter;

import com.devin.pipeline.core.trace.ExecutionTrace;
import com.devin.pipeline.core.trace.StageRecord;
import com.devin.pipeline.core.trace.TraceExporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes {@code trace-<id>.json} into the configured directory for machines and log platforms.
 */
public final class JsonTraceExporter implements TraceExporter {

    private final Path directory;

    public JsonTraceExporter() {
        this(Path.of("out", "traces"));
    }

    public JsonTraceExporter(Path directory) {
        this.directory = directory == null ? Path.of("out", "traces") : directory;
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
            System.err.println("[JsonTraceExporter] export failed for " + trace.traceId() + ": " + e);
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
