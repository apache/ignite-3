package org.apache.ignite.internal.table.distributed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableImpl;

public class TableProcessorsStorage {

    // (zoneId -> set<TableImpl>)
    private final ConcurrentHashMap<Integer, Set<TableImpl>> zones = new ConcurrentHashMap<>();

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    public TableProcessorsStorage(PartitionReplicaLifecycleManager partitionReplicaLifecycleManager) {
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
    }

    // can be synch
    public CompletionStage<Void> forEveryTableFromZoneIfNotExists(int zoneId, int partid, Function<TableImpl, CompletableFuture<Void>> tablePartitionLoader) {
        List<CompletableFuture<?>> fut = new ArrayList<>();

        zones.computeIfPresent(zoneId, (id, tables) -> {
            tables.forEach(t -> {
                fut.add(tablePartitionLoader.apply(t));
            });

            return tables;
        });

        return CompletableFuture.allOf(fut.toArray(new CompletableFuture[]{}));
    }

    public CompletableFuture<Void> initTableForZoneAndStartNeededPartitions(int zoneId, TableImpl table, int partitions, Function<Integer, CompletableFuture<Void>> partitionInitializer) {

        final CompletableFuture<Void>[] fut = new CompletableFuture<Void>[1];

        zones.compute(zoneId, (id, tables) -> {
            Set<TableImpl> actualTables = tables;

            if (actualTables == null) {
                actualTables = new HashSet<>();
            }

            assert !actualTables.contains(table);


            fut[0] = partitionReplicaLifecycleManager.forAllPartitions(zoneId, (p) -> partitionInitializer.apply(p));

            actualTables.add(table);

            return actualTables;
        });

        return fut[0];

    }
}
