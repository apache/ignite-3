package org.apache.ignite.internal.traced;

import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.traced.TraceContext.TraceEntry;

public class TracedException extends RuntimeException {
    private final List<TraceEntry> trace;

    public List<TraceContext.TraceEntry> trace() {
        return trace;
    }

    public TracedException(List<TraceContext.TraceEntry> trace, Throwable cause) {
        super(cause);
        this.trace = trace;
    }

    @Override
    public String toString() {
        StringBuilder t = new StringBuilder();
        for (TraceContext.TraceEntry entry : trace) {
            t.append("\n- ").append(entry);
        }
        return super.toString() + ",\nwith trace: " + t;
    }
}
