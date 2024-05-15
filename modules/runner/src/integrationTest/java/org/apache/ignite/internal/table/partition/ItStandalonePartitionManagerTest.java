package org.apache.ignite.internal.table.partition;

import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;

public class ItStandalonePartitionManagerTest extends ItAbstractPartitionManagerTest {
    @Override
    protected PartitionManager partitionManager() {
        return cluster.aliveNode().tables().table(TABLE_NAME).partitionManager();
    }

    @Override
    protected Partition toPartition(int i) {
        return new HashPartition(i);
    }
}
