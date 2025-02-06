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

/**
 * Manages resources of distribution zones; that is, allows creation of underlying storages and closes them on node stop.
 */
class ZoneResourcesManager implements ManuallyCloseable {
    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    private final Map<Integer, Integer> zonePartitionCounts = new ConcurrentHashMap<>();

    private final Map<Integer, ZoneResources> zoneResourcesMap = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    ZoneResourcesManager(TxStateRocksDbSharedStorage sharedTxStateStorage) {
        this.sharedTxStateStorage = sharedTxStateStorage;
    }

    /**
     * Registers zone partition count. This must be called before {@link #getOrCreatePartitionStorage(int, int)}.
     *
     * @param zoneId ID of the zone.
     * @param partitionCount Number of partitions the zone has.
     */
    void registerZonePartitionCount(int zoneId, int partitionCount) {
        zonePartitionCounts.put(zoneId, partitionCount);
    }

    /**
     * Gets or creates a transaction state storage for a zone partition. {@link #registerZonePartitionCount(int, int)} must be called before
     * calling this method.
     *
     * @param zoneId ID of the zone.
     * @param partitionId Partition ID.
     */
    TxStatePartitionStorage getOrCreatePartitionStorage(int zoneId, int partitionId) {
        return inBusyLock(busyLock, () -> {
            Integer partitionCount = zonePartitionCounts.put(zoneId, partitionId);
            assert partitionCount != null : "No partition count was registered for zone " + zoneId
                    + "; make sure to register it when handling node startup and zone creation";

            ZoneResources zoneResources = zoneResourcesMap.computeIfAbsent(
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

        for (ZoneResources zoneResources : zoneResourcesMap.values()) {
            zoneResources.txStateStorage.close();
        }
    }

    private static class ZoneResources {
        private final TxStateStorage txStateStorage;

        private ZoneResources(TxStateStorage txStateStorage) {
            this.txStateStorage = txStateStorage;
        }
    }
}
