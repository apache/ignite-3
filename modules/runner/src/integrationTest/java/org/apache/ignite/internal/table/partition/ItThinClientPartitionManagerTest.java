package org.apache.ignite.internal.table.partition;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.table.ClientPartitionManager.ClientHashPartition;
import org.apache.ignite.table.partition.Partition;
import org.apache.ignite.table.partition.PartitionManager;
import org.junit.jupiter.api.BeforeEach;

public class ItThinClientPartitionManagerTest extends ItAbstractPartitionManagerTest {
    private IgniteClient client;

    @BeforeEach
    public void startClient() {
        client = IgniteClient.builder()
                .addresses("localhost")
                .reconnectThrottlingPeriod(0)
                .build();
    }

    @Override
    protected PartitionManager partitionManager() {
        return client.tables().table(TABLE_NAME).partitionManager();
    }

    @Override
    protected Partition toPartition(int i) {
        return new ClientHashPartition(i);
    }
}
