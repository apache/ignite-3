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
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.tx.storage.state.ThreadAssertingTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.worker.ThreadAssertions;

class ZoneResourcesManager implements ManuallyCloseable {
    private final TxStateRocksDbSharedStorage sharedTxStateStorage;

    private final Map<Integer, ZoneResources> zoneResourcesMap = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    ZoneResourcesManager(TxStateRocksDbSharedStorage sharedTxStateStorage) {
        this.sharedTxStateStorage = sharedTxStateStorage;
    }

    /**
     * Gets or creates a transaction state storage for a zone partition.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param partitionId Partition ID.
     */
    TxStatePartitionStorage getOrCreatePartitionStorage(CatalogZoneDescriptor zoneDescriptor, int partitionId) {
        return inBusyLock(busyLock, () -> {
            ZoneResources zoneResources = zoneResourcesMap.computeIfAbsent(
                    zoneDescriptor.id(),
                    zoneId -> createZoneResources(zoneDescriptor)
            );

            return zoneResources.txStateStorage.getOrCreatePartitionStorage(partitionId);
        });
    }

    private ZoneResources createZoneResources(CatalogZoneDescriptor zoneDescriptor) {
        return new ZoneResources(createTxStateStorage(zoneDescriptor));
    }

    private TxStateStorage createTxStateStorage(CatalogZoneDescriptor zoneDescriptor) {
        TxStateStorage txStateStorage = new TxStateRocksDbStorage(
                zoneDescriptor.id(),
                zoneDescriptor.partitions(),
                sharedTxStateStorage
        );

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
