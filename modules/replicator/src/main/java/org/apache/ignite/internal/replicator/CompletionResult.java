package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;

// TODO refactor
public class CompletionResult {
    private final Object res;
    private final Object delayedRes;

    public CompletionResult(Object res, Object delayedRes) {
        this.res = res;
        this.delayedRes = delayedRes;
    }

    public Object result() {
        return res;
    }

    public Object delayedResult() {
        return delayedRes;
    }
}
