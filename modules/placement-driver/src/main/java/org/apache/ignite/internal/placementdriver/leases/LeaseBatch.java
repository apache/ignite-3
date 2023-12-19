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

package org.apache.ignite.internal.placementdriver.leases;

import static org.apache.ignite.internal.util.IgniteUtils.bytesToList;
import static org.apache.ignite.internal.util.IgniteUtils.collectionToBytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Representation of leases batch.
 */
public class LeaseBatch {
    private final Collection<Lease> leases;

    public LeaseBatch(Collection<Lease> leases) {
        this.leases = leases;
    }

    public Collection<Lease> leases() {
        return leases;
    }

    public byte[] bytes() {
        return collectionToBytes(leases, Lease::bytes);
    }

    public static LeaseBatch fromBytes(ByteBuffer bytes) {
        return new LeaseBatch(bytesToList(bytes, Lease::fromBytes));
    }

    public Leases toMessage() {
        var msgFactory = new PlacementDriverImplFactory();

        Map<String, Integer> leaseholders = new LinkedHashMap<>();
        Map<String, Integer> leaseholderIds = new LinkedHashMap<>();

        for (Lease lease : leases) {
            leaseholders.putIfAbsent(lease.getLeaseholder(), leaseholders.size());
            leaseholderIds.putIfAbsent(lease.getLeaseholderId(), leaseholderIds.size());
        }

        class Partitions {
            int maxPartitionId = -1;

            List<Lease> leases = new ArrayList<>();
        }

        Map<Integer, Partitions> leasesByTableId = new HashMap<>();

        for (Lease lease : leases) {
            TablePartitionId tablePartitionId = (TablePartitionId) lease.replicationGroupId();
            Partitions partitions = leasesByTableId.computeIfAbsent(tablePartitionId.tableId(), key -> new Partitions());

            partitions.leases.add(lease);
            partitions.maxPartitionId = Math.max(partitions.maxPartitionId, tablePartitionId.partitionId());
        }

        Map<Integer, PartitionLeases> messageMap = new HashMap<>();

        for (Entry<Integer, Partitions> entry : leasesByTableId.entrySet()) {
            Partitions partitions = entry.getValue();

            PartitionLease[] partitionLeasesArray = new PartitionLease[partitions.maxPartitionId + 1];
            for (Lease lease : partitions.leases) {
                assert lease.getExpirationTime().getLogical() == 0;

                PartitionLease partitionLease = msgFactory.partitionLease()
                        .leaseholder(leaseholders.get(lease.getLeaseholder()))
                        .leaseholderId(leaseholderIds.get(lease.getLeaseholderId()))
                        .flags((lease.isAccepted() ? 1 : 0) | (lease.isProlongable() ? 2 : 0))
                        .startTimePhysical(lease.getStartTime().getPhysical())
                        .startTimeLogical(lease.getStartTime().getLogical())
                        .delta(lease.getExpirationTime().getPhysical() - lease.getStartTime().getPhysical())
                        .build();

                partitionLeasesArray[((TablePartitionId) lease.replicationGroupId()).partitionId()] = partitionLease;
            }

            messageMap.put(entry.getKey(), msgFactory.partitionLeases().leases(partitionLeasesArray).build());
        }

        return msgFactory.leases()
                .leaseholders(new ArrayList<>(leaseholders.keySet()))
                .leaseholderIds(new ArrayList<>(leaseholderIds.keySet()))
                .leases(messageMap)
                .build();
    }

    public static LeaseBatch fromMessage(Leases message) {
        List<Lease> leases = new ArrayList<>();

        for (Entry<Integer, PartitionLeases> entry : message.leases().entrySet()) {
            int tableId = entry.getKey();
            PartitionLeases tableLeases = entry.getValue();

            PartitionLease[] array = tableLeases.leases();
            for (int partId = 0; partId < array.length; partId++) {
                PartitionLease partitionLease = array[partId];

                if (partitionLease != null) {
                    leases.add(new Lease(
                            message.leaseholders().get(partitionLease.leaseholder()),
                            message.leaseholderIds().get(partitionLease.leaseholderId()),
                            new HybridTimestamp(partitionLease.startTimePhysical(), partitionLease.startTimeLogical()),
                            new HybridTimestamp(partitionLease.startTimePhysical() + partitionLease.delta(), 0),
                            (partitionLease.flags() & 2) != 0,
                            (partitionLease.flags() & 1) != 0,
                            new TablePartitionId(tableId, partId)
                    ));
                }
            }
        }

        return new LeaseBatch(leases);
    }

    @Transferable(0)
    public interface Leases extends NetworkMessage {
        List<String> leaseholders();
        List<String> leaseholderIds();
        Map<Integer, PartitionLeases> leases();
    }

    @Transferable(1)
    public interface PartitionLeases extends NetworkMessage {
        PartitionLease[] leases();
    }

    @Transferable(2)
    public interface PartitionLease extends NetworkMessage {
        int leaseholder();
        int leaseholderId();
        int flags();
        long startTimePhysical();
        int startTimeLogical();
        long delta();
    }
}
