package org.apache.ignite.internal.sql.engine;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.ExecutionDistributionProviderImpl.DistributionHolder;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;

@FunctionalInterface
public interface ExecutionDistributionProvider {
    CompletableFuture<DistributionHolder> distribution(
            HybridTimestamp operationTime,
            boolean mapOnBackups,
            Collection<IgniteTable> tables,
            List<String> viewNodes,
            String initiatorNode);
}
