package org.apache.ignite.internal.table;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

public interface StreamerReceiverRunner {
    public @Nullable CompletableFuture<List<Object>> runAsync(byte[] payload);
}
