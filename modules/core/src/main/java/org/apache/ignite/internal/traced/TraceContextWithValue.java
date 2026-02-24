package org.apache.ignite.internal.traced;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

public class TraceContextWithValue<T> {
    private final TraceContext traceContext;
    private final T value;
    private final @Nullable Throwable throwable;

    public TraceContextWithValue(TraceContext traceContext, T value, @Nullable Throwable throwable) {
        this.traceContext = traceContext;
        this.value = value;
        this.throwable = throwable;
    }

    public TraceContext traceContext() {
        return traceContext;
    }

    public T value() {
        return value;
    }

    @Nullable
    public Throwable throwable() {
        return throwable;
    }

    public TracedFuture<T> section(String name) {
        traceContext.startTrace(name);
        return TracedFuture.continueWithContext(CompletableFuture.completedFuture(value), traceContext);
    }
}
