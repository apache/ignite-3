package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;

public class CompletionResult {
    private final Object res;
    private final CompletableFuture<?> delayedRes;

    public CompletionResult(Object res, CompletableFuture<?> delayedRes) {
        this.res = res;
        this.delayedRes = delayedRes;
    }

    public CompletableFuture<?> delayedResult() {
        return delayedRes;
    }

    public Object result() {
        return res;
    }
}
