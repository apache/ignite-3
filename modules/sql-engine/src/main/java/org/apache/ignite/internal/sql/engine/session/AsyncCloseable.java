package org.apache.ignite.internal.sql.engine.session;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface AsyncCloseable {
    CompletableFuture<Void> closeAsync();
}
