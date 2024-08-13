package org.apache.ignite.internal.table.distributed;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.table.TableImpl;

public class TableProcessorsStorage {

    // (zoneId -> set<TableImpl>)
    private final ConcurrentHashMap<Integer, Set<TableImpl>> zones = new ConcurrentHashMap<>();

    public TableProcessorsStorage(PartitionReplicaLifecycleManager partitionReplicaLifecycleManager) {
    }

    public Set<TableImpl> tables(int zoneId) {
        return zones.compute(zoneId, (id, tables) -> {
            if (tables == null) {
                tables = new HashSet<>();
            }

            return tables;
        });
    }

    public void add(int zoneId, TableImpl table) {
        zones.compute(zoneId, (id, tbls) -> {
            if (tbls == null) {
                tbls = new HashSet<>();
            }

            tbls.add(table);

            return tbls;
        });
    }
}
