package org.apache.ignite.internal.table.distributed.disaster;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

class ManualGroupRestartRequest implements DisasterRecoveryRequest {
    private static final long serialVersionUID = 0L;

    private final UUID operationId;

    private final int zoneId;

    private final int tableId;

    private final Set<Integer> partitionIds;

    ManualGroupRestartRequest(UUID operationId, int zoneId, int tableId, Set<Integer> partitionIds) {
        this.operationId = operationId;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionIds = Set.copyOf(partitionIds);
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    public int tableId() {
        return tableId;
    }

    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    @Override
    public CompletableFuture<Void> handle(
            DisasterRecoveryManager disasterRecoveryManager,
            long revision,
            CompletableFuture<Void> operationFuture
    ) {
        return null;
    }
}
