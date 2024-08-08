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
    private final ConcurrentHashMap<Integer, Set<TableImpl>> zones;

    // (tableId -> partitionSet)
    private final ConcurrentHashMap<Integer, PartitionSet> tablePartitions;

    private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    public CompletionStage<Void> forEveryTableFromZoneIfNotExists(int zoneId, int partid, Function<TableImpl, CompletableFuture<Void>> tablePartitionLoader) {
        List<CompletableFuture<?>> fut = new ArrayList<>();

        zones.computeIfPresent(zoneId, (id, tables) -> {
            tables.forEach(t -> {
                tablePartitions.compute(t.tableId(), (tableId, partitionSet) -> {
                    // KKK: do we need to check if partitionSet have this partition already?
                    fut.add(tablePartitionLoader.apply(t));

                    if (partitionSet == null) {
                        partitionSet = new BitSetPartitionSet();
                        partitionSet.set(partid);
                    } else {
                        partitionSet.set(partid);
                    }

                    return partitionSet;
                });
            });

            return tables;
        });

        return CompletableFuture.allOf(fut.toArray(new CompletableFuture[]{}));
    }

    public CompletionStage<Void> initTableForZoneAndStartNeededPartitions(int zoneId, TableImpl table, int partitions, Function<Integer, CompletableFuture<Void>> partitionInitializer) {


        List<CompletableFuture<?>> fut = new ArrayList<>();

        zones.compute(zoneId, (id, tables) -> {
            Set<TableImpl> actualTables = tables;

            if (actualTables == null) {
                actualTables = new HashSet<>();
            }

            assert !actualTables.contains(table);

            PartitionSet partitionSet = new BitSetPartitionSet();

            for (int p = 0; p < partitions; p++) {
                if (partitionReplicaLifecycleManager.hasLocalPartition(new ZonePartitionId(zoneId, p))) {
                    fut.add(partitionInitializer.apply(p));

                    partitionSet.set(p);
                }
            }

            tablePartitions.put(table.tableId(), partitionSet);

            actualTables.add(table);

            return actualTables;
        });

        return CompletableFuture.allOf(fut.toArray(new CompletableFuture[]{}));

    }
}
