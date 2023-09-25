package org.apache.ignite.internal.replicator;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Represents replica execution result.
 */
public class ReplicaResult {
    private final Object res;
    private final CompletableFuture<?> repFut;

    /**
     * @param res The result.
     * @param repFut The replication future.
     */
    public ReplicaResult(@Nullable Object res, @Nullable CompletableFuture<?> repFut) {
        this.res = res;
        this.repFut = repFut;
    }

    /**
     * @return The result.
     */
    public @Nullable Object result() {
        return res;
    }

    /**
     * @return The replication future.
     */
    public @Nullable CompletableFuture<?> repFuture() {
        return repFut;
    }
}
