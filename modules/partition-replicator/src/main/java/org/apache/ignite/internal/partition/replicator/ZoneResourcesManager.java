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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.TestOnly;

/**
 * Manages resources of distribution zones; that is, allows creation of underlying storages and closes them on node stop.
 */
class ZoneResourcesManager implements ManuallyCloseable {
    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    /** Map from zone IDs to their resource holders. */
    private final Map<Integer, ZoneResources> resourcesByZoneId = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    ZoneResourcesManager(TxStateRocksDbSharedStorage sharedTxStateStorage) {
        this.sharedTxStateStorage = sharedTxStateStorage;
    }

    /**
     * Gets or creates a transaction state storage for a zone partition.
     *
     * @param zoneId ID of the zone.
     * @param partitionCount Number of partitions in the zone.
     * @param partitionId Partition ID.
     */
    TxStatePartitionStorage getOrCreatePartitionTxStateStorage(int zoneId, int partitionCount, int partitionId) {
        return inBusyLock(busyLock, () -> {
            ZoneResources zoneResources = resourcesByZoneId.computeIfAbsent(
                    zoneId,
                    id -> createZoneResources(id, partitionCount)
            );

            return zoneResources.txStateStorage.getOrCreatePartitionStorage(partitionId);
        });
    }

    private ZoneResources createZoneResources(int zoneId, int partitionCount) {
        return new ZoneResources(createTxStateStorage(zoneId, partitionCount));
    }

    private TxStateStorage createTxStateStorage(int zoneId, int partitionCount) {
        TxStateStorage txStateStorage = new TxStateRocksDbStorage(zoneId, partitionCount, sharedTxStateStorage);

        if (ThreadAssertions.enabled()) {
            txStateStorage = new ThreadAssertingTxStateStorage(txStateStorage);
        }

        txStateStorage.start();

        return txStateStorage;
    }

    @Override
    public void close() {
        busyLock.block();

        for (ZoneResources zoneResources : resourcesByZoneId.values()) {
            zoneResources.txStateStorage.close();
        }
    }

    private static class ZoneResources {
        private final TxStateStorage txStateStorage;

        private ZoneResources(TxStateStorage txStateStorage) {
            this.txStateStorage = txStateStorage;
        }
    }

    @TestOnly
    public TxStatePartitionStorage txStatePartitionStorage(int zoneId, int partitionId) {
        ZoneResources resources = resourcesByZoneId.get(zoneId);
        assert resources != null : "No resources yet for zone " + zoneId;

        TxStatePartitionStorage partitionStorage = resources.txStateStorage.getPartitionStorage(partitionId);
        assert partitionStorage != null : "No partition storage for zone " + zoneId + " and partition " + partitionId;

        return partitionStorage;
    }
}
