/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx.storage.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract tx state storage test.
 */
public abstract class AbstractTxStateStorageTest extends BaseIgniteAbstractTest {
    protected static final int ZONE_ID = 1;

    protected static final byte[] GROUP_CONFIGURATION = {1, 2, 3};

    protected static final byte[] SNAPSHOT_INFO = {4, 5, 6};

    protected static final LeaseInfo LEASE_INFO = new LeaseInfo(1, UUID.randomUUID(), "node");

    protected TxStateStorage txStateStorage;

    /**
     * Creates {@link TxStatePartitionStorage} to test.
     */
    protected abstract TxStateStorage createTableOrZoneStorage();

    @BeforeEach
    protected void beforeTest() {
        createAndStartStorage();
    }

    private void createAndStartStorage() {
        txStateStorage = createTableOrZoneStorage();

        txStateStorage.start();
    }

    @AfterEach
    protected void afterTest() throws Exception {
        txStateStorage.close();
    }

    @Test
    public void partitionDestructionRemovesAllDataAndMetadata() {
        int partitionIndex = 0;

        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(partitionIndex);

        partitionStorage.committedGroupConfiguration(GROUP_CONFIGURATION, 1, 1);
        partitionStorage.leaseInfo(LEASE_INFO, 2, 1);
        partitionStorage.snapshotInfo(SNAPSHOT_INFO);

        TxMeta txMeta = new TxMeta(TxState.ABORTED, List.of(), null);
        partitionStorage.putForRebalance(UUID.randomUUID(), txMeta);

        txStateStorage.destroyPartitionStorage(partitionIndex);

        assertNull(txStateStorage.getPartitionStorage(partitionIndex));

        TxStatePartitionStorage newPartitionStorage = txStateStorage.getOrCreatePartitionStorage(partitionIndex);

        assertThat(newPartitionStorage.lastAppliedIndex(), is(0L));
        assertThat(newPartitionStorage.lastAppliedTerm(), is(0L));
        assertThat(newPartitionStorage.committedGroupConfiguration(), is(nullValue()));
        assertThat(newPartitionStorage.leaseInfo(), is(nullValue()));
        assertThat(newPartitionStorage.snapshotInfo(), is(nullValue()));
    }

    @Test
    public void wholeDestructionRemovesAllDataAndMetadata() {
        int partitionIndex = 0;

        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(partitionIndex);

        partitionStorage.committedGroupConfiguration(GROUP_CONFIGURATION, 1, 1);
        partitionStorage.leaseInfo(LEASE_INFO, 2, 1);
        partitionStorage.snapshotInfo(SNAPSHOT_INFO);

        TxMeta txMeta = new TxMeta(TxState.ABORTED, List.of(), null);
        partitionStorage.putForRebalance(UUID.randomUUID(), txMeta);

        txStateStorage.destroy();
        createAndStartStorage();

        assertNull(txStateStorage.getPartitionStorage(partitionIndex));

        TxStatePartitionStorage newPartitionStorage = txStateStorage.getOrCreatePartitionStorage(partitionIndex);

        assertThat(newPartitionStorage.lastAppliedIndex(), is(0L));
        assertThat(newPartitionStorage.lastAppliedTerm(), is(0L));
        assertThat(newPartitionStorage.committedGroupConfiguration(), is(nullValue()));
        assertThat(newPartitionStorage.leaseInfo(), is(nullValue()));
        assertThat(newPartitionStorage.snapshotInfo(), is(nullValue()));
    }
}
