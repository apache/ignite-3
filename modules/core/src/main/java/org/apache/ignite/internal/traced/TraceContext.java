package org.apache.ignite.internal.traced;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class TraceContext {
    private final List<TraceEntry> entries = new CopyOnWriteArrayList<>();

    public void startTrace(String name) {
        entries.add(new TraceEntry(name, System.currentTimeMillis(), null));
    }

    public void startTrace(String name, Throwable throwable) {
        entries.add(new TraceEntry(name, System.currentTimeMillis(), throwable));
    }

    public List<TraceEntry> traceEntries() {
        return entries;
    }

    public void addChildToLastEntry(TraceContext child) {
        if (entries.isEmpty()) {
            entries.add(new TraceEntry("entry-" + UUID.randomUUID(), System.currentTimeMillis(), null));
        }

        entries.get(entries.size() - 1).addChild(child);
    }

    public String packToString() {
        StringBuilder sb = new StringBuilder();
        writeContext(sb, this);
        return sb.toString();
    }

    private static void writeContext(StringBuilder out, TraceContext ctx) {
        out.append('C').append(' ').append(ctx.entries.size()).append('\n');
        for (TraceEntry e : ctx.entries) {
            writeEntry(out, e);
        }
    }

    private static void writeEntry(StringBuilder out, TraceEntry e) {
        String name = e.name == null ? "" : e.name;
        int nameLen = name.length();
        int childCount = (e.children == null) ? 0 : e.children.size();

        out.append('E').append(' ')
                .append(e.startTimeMillis).append(' ')
                .append(nameLen).append(' ')
                .append(childCount).append('\n');

        // имя пишем как raw-блок фиксированной длины (может содержать пробелы/переносы/что угодно)
        out.append(name);

        if (childCount > 0) {
            for (TraceContext child : e.children) {
                writeContext(out, child);
            }
        }
    }

    public static TraceContext unpackFromString(String s) {
        Cursor c = new Cursor(s);
        TraceContext ctx = readContext(c);

        // если в конце есть мусор — это повод упасть, чтобы сразу увидеть проблему формата
        if (!c.eof()) {
            // можно смягчить и просто игнорить хвост, но лучше ловить баги рано
            throw new IllegalStateException("Trailing data at pos " + c.pos);
        }
        return ctx;
    }

    private static TraceContext readContext(Cursor c) {
        c.expect('C');
        c.expect(' ');
        int entryCount = c.readIntUntil('\n');

        TraceContext ctx = new TraceContext();
        for (int i = 0; i < entryCount; i++) {
            ctx.entries.add(readEntry(c));
        }
        return ctx;
    }

    private static TraceEntry readEntry(Cursor c) {
        c.expect('E');
        c.expect(' ');
        long start = c.readLongUntil(' ');
        int nameLen = c.readIntUntil(' ');
        int childCount = c.readIntUntil('\n');

        String name = c.readChars(nameLen);

        TraceEntry e = new TraceEntry(name, start, null);

        for (int i = 0; i < childCount; i++) {
            TraceContext child = readContext(c);
            e.addChild(child);
        }
        return e;
    }

    private static final class Cursor {
        final String s;
        int pos;

        Cursor(String s) {
            this.s = s;
            this.pos = 0;
        }

        boolean eof() {
            return pos >= s.length();
        }

        void expect(char ch) {
            if (pos >= s.length() || s.charAt(pos) != ch) {
                String got = (pos >= s.length()) ? "<eof>" : String.valueOf(s.charAt(pos));
                throw new IllegalStateException("Expected '" + ch + "' at pos " + pos + ", got " + got);
            }
            pos++;
        }

        int readIntUntil(char delimiter) {
            long v = readLongUntil(delimiter);
            if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
                throw new IllegalStateException("Int overflow at pos " + pos + ": " + v);
            }
            return (int) v;
        }

        long readLongUntil(char delimiter) {
            if (pos >= s.length()) throw new IllegalStateException("Unexpected eof at pos " + pos);

            boolean neg = false;
            if (s.charAt(pos) == '-') {
                neg = true;
                pos++;
            }

            long val = 0;
            boolean any = false;

            while (pos < s.length()) {
                char ch = s.charAt(pos);
                if (ch == delimiter) {
                    pos++; // съели delimiter
                    break;
                }
                if (ch < '0' || ch > '9') {
                    throw new IllegalStateException("Bad number char '" + ch + "' at pos " + pos);
                }
                any = true;
                val = val * 10 + (ch - '0');
                pos++;
            }

            if (!any) throw new IllegalStateException("Empty number at pos " + pos);

            return neg ? -val : val;
        }

        String readChars(int n) {
            if (n < 0) throw new IllegalStateException("Negative length: " + n);
            if (pos + n > s.length()) {
                throw new IllegalStateException("Not enough chars: need " + n + " at pos " + pos);
            }
            String out = s.substring(pos, pos + n);
            pos += n;
            return out;
        }
    }

    public static class TraceEntry {
        final String name;
        final long startTimeMillis;
        final Throwable throwable;
        List<TraceContext> children;

        private TraceEntry(String name, long startTimeMillis, Throwable throwable) {
            this.name = name;
            this.startTimeMillis = startTimeMillis;
            this.throwable = throwable;
        }

        void addChild(TraceContext child) {
            if (children == null) {
                children = new CopyOnWriteArrayList<>();
            }
            children.add(child);
        }

        @Override
        public String toString() {
            return name + " started at " + startTimeMillis + " ms"
                    + exceptionalTrace()
                    + printChildrenIfPresent();
        }

        private String printChildrenIfPresent() {
            if (children == null) {
                return "";
            }

            StringBuilder s = new StringBuilder();

            for (int i = 0; i < children.size(); i++) {
                s.append("\n  child " + i);
                TraceContext child = children.get(i);

                StringBuilder cb = new StringBuilder();
                for (TraceContext.TraceEntry entry : child.entries) {
                    cb.append("\n- ").append(entry);
                }
                String c = cb.toString();

                c = c.replaceAll("\n", "\n    ");
                s.append(c);
            }
            return s.toString();
        }

        private String exceptionalTrace() {
            return throwable == null
                    ? ""
                    : " (entered with throwable: " + throwable.getMessage() + ")";
        }
    }
}
