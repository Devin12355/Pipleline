package com.devin.pipeline.dsl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A deliberately small, dependency-free YAML subset parser - just enough for pipeline documents:
 *
 * <ul>
 *   <li>block-style mappings ({@code key: value}) and sequences ({@code - item})</li>
 *   <li>scalars: bare strings, "quoted strings", integers, decimals, true/false, null/~</li>
 *   <li>{@code #} comments, blank lines, documents separated by {@code ---} are ignored</li>
 * </ul>
 *
 * <p>Not supported (by design): anchors/aliases, multi-line scalars ({@code |} / {@code >}),
 * flow collections ({@code {a: 1}} / {@code [1,2]}), tags.
 */
final class MiniYaml {

    private static final class Line {
        final int indent;
        final String content;
        final int number;

        Line(int indent, String content, int number) {
            this.indent = indent;
            this.content = content;
            this.number = number;
        }
    }

    private static final class Cursor {
        final List<Line> lines;
        int pos;

        Cursor(List<Line> lines) {
            this.lines = lines;
        }

        Line peek() {
            return pos < lines.size() ? lines.get(pos) : null;
        }
    }

    private MiniYaml() {
    }

    /** Parses a document into {@code Map<String,Object>} / {@code List<Object>} / scalars. */
    static Object parse(String text) {
        if (text == null || text.isBlank()) {
            return new LinkedHashMap<String, Object>();
        }
        List<Line> lines = new ArrayList<>();
        String[] raw = text.replace("\r\n", "\n").split("\n", -1);
        for (int i = 0; i < raw.length; i++) {
            String stripped = stripComment(raw[i]);
            if (stripped.isBlank()) {
                continue;
            }
            int indent = 0;
            while (indent < stripped.length() && stripped.charAt(indent) == ' ') {
                indent++;
            }
            if (indent < stripped.length() && stripped.charAt(indent) == '\t') {
                throw error(i + 1, "tabs are not supported, indent with spaces");
            }
            String content = stripped.substring(indent);
            if (content.equals("---") || content.equals("...")) {
                continue;
            }
            lines.add(new Line(indent, content, i + 1));
        }
        if (lines.isEmpty()) {
            return new LinkedHashMap<String, Object>();
        }
        Cursor cursor = new Cursor(lines);
        Object node = parseNode(cursor, lines.get(0).indent);
        Line rest = cursor.peek();
        if (rest != null) {
            throw error(rest.number, "unexpected content: " + rest.content);
        }
        return node;
    }

    private static Object parseNode(Cursor cursor, int indent) {
        Line line = cursor.peek();
        if (isDash(line.content)) {
            return parseList(cursor, indent);
        }
        return parseMap(cursor, indent);
    }

    private static Map<String, Object> parseMap(Cursor cursor, int indent) {
        Map<String, Object> map = new LinkedHashMap<>();
        while (true) {
            Line line = cursor.peek();
            if (line == null || line.indent < indent || isDash(line.content)) {
                return map;
            }
            if (line.indent > indent) {
                throw error(line.number, "unexpected indentation: " + line.content);
            }
            int colon = findColon(line.content);
            if (colon < 0) {
                throw error(line.number, "expected 'key: value': " + line.content);
            }
            String key = unquote(line.content.substring(0, colon).trim());
            String rest = line.content.substring(colon + 1).trim();
            cursor.pos++;
            if (rest.isEmpty()) {
                map.put(key, parseChildBlock(cursor, indent));
            } else {
                map.put(key, scalar(rest, line.number));
            }
        }
    }

    /** Value of a {@code key:} with nothing after the colon: nested block, or null. */
    private static Object parseChildBlock(Cursor cursor, int keyIndent) {
        Line next = cursor.peek();
        if (next == null) {
            return null;
        }
        if (next.indent > keyIndent) {
            return parseNode(cursor, next.indent);
        }
        if (next.indent == keyIndent && isDash(next.content)) {
            return parseList(cursor, keyIndent);
        }
        return null;
    }

    private static List<Object> parseList(Cursor cursor, int indent) {
        List<Object> list = new ArrayList<>();
        while (true) {
            Line line = cursor.peek();
            if (line == null || line.indent < indent || !isDash(line.content)) {
                return list;
            }
            if (line.indent > indent) {
                throw error(line.number, "unexpected indentation: " + line.content);
            }
            if (line.content.equals("-")) {
                cursor.pos++;
                Line next = cursor.peek();
                if (next != null && next.indent > indent) {
                    list.add(parseNode(cursor, next.indent));
                } else {
                    list.add(null);
                }
            } else {
                // "- key: value" -> re-align as if the item content were a normal line
                String rest = line.content.substring(2).trim();
                int childIndent = line.indent + (line.content.length() - rest.length());
                cursor.lines.set(cursor.pos, new Line(childIndent, rest, line.number));
                list.add(parseNode(cursor, childIndent));
            }
        }
    }

    private static Object scalar(String raw, int lineNo) {
        String s = raw.trim();
        if (s.isEmpty() || s.equals("null") || s.equals("~")) {
            return null;
        }
        if (s.length() >= 2 && ((s.startsWith("\"") && s.endsWith("\""))
                || (s.startsWith("'") && s.endsWith("'")))) {
            return s.substring(1, s.length() - 1);
        }
        if (s.equals("true")) {
            return Boolean.TRUE;
        }
        if (s.equals("false")) {
            return Boolean.FALSE;
        }
        if (s.matches("[+-]?\\d+")) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                return s;
            }
        }
        if (s.matches("[+-]?\\d+\\.\\d+([eE][+-]?\\d+)?")) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                return s;
            }
        }
        if (s.contains(": ") || s.endsWith(":")) {
            throw error(lineNo, "nested inline mappings are not supported, use block style: " + raw);
        }
        return s;
    }

    private static int findColon(String s) {
        boolean quoted = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == ':' && !quoted && (i + 1 == s.length() || s.charAt(i + 1) == ' ')) {
                return i;
            }
        }
        return -1;
    }

    private static String stripComment(String line) {
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == '#' && !quoted && (i == 0 || line.charAt(i - 1) == ' ')) {
                return line.substring(0, i);
            }
        }
        return line;
    }

    private static boolean isDash(String content) {
        return content.equals("-") || content.startsWith("- ");
    }

    private static String unquote(String s) {
        if (s.length() >= 2 && ((s.startsWith("\"") && s.endsWith("\""))
                || (s.startsWith("'") && s.endsWith("'")))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static IllegalArgumentException error(int lineNo, String message) {
        return new IllegalArgumentException("YAML line " + lineNo + ": " + message);
    }
}
