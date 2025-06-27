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

package org.apache.ignite.internal.placementdriver;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for lease tracker.
 */
public class LeaseTrackerTest extends BaseIgniteAbstractTest {
    @Test
    public void testLeaseCleanup() {
        AtomicReference<WatchListener> listenerRef = new AtomicReference<>();
        MetaStorageManager msManager = mock(MetaStorageManager.class);

        doAnswer(
                invocation -> {
                    WatchListener lsnr = invocation.getArgument(1);
                    listenerRef.set(lsnr);
                    return null;
                }
        ).when(msManager).registerExactWatch(any(), any());

        byte[] leasesKeyBytes = PLACEMENTDRIVER_LEASES_KEY.bytes();
        Entry emptyEntry = EntryImpl.empty(leasesKeyBytes);

        when(msManager.getLocally(any(), anyLong())).thenAnswer(invocation -> emptyEntry);

        HybridClockImpl clock = new HybridClockImpl();

        LeaseTracker leaseTracker = new LeaseTracker(
                msManager,
                mock(ClusterNodeResolver.class),
                new TestClockService(clock)
        );
        leaseTracker.startTrack(0L);

        AtomicReference<PrimaryReplicaEventParameters> parametersRef = new AtomicReference<>();
        leaseTracker.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, p -> {
            parametersRef.set(p);
            return falseCompletedFuture();
        });

        TablePartitionId partId0 = new TablePartitionId(0, 0);
        TablePartitionId partId1 = new TablePartitionId(0, 1);

        HybridTimestamp startTime = new HybridTimestamp(1, 0);
        HybridTimestamp expirationTime = new HybridTimestamp(1000, 0);

        String leaseholder0 = "notAccepted";
        String leaseholder1 = "accepted";

        Lease lease0 = new Lease(leaseholder0, randomUUID(), startTime, expirationTime, partId0);
        Lease lease1 = new Lease(leaseholder1, randomUUID(), startTime, expirationTime, partId1)
                .acceptLease(new HybridTimestamp(2000, 0));

        // In entry0, there are leases for partition ids partId0 and partId1. In entry1, there is only partId0, so partId1 is expired.
        Entry entry0 = new EntryImpl(leasesKeyBytes, new LeaseBatch(List.of(lease0, lease1)).bytes(), 0, clock.now());
        Entry entry1 = new EntryImpl(leasesKeyBytes, new LeaseBatch(List.of(lease0)).bytes(), 0, clock.now());
        listenerRef.get().onUpdate(new WatchEvent(new EntryEvent(emptyEntry, entry0)));

        assertNull(parametersRef.get());

        // Check that the absence of accepted lease triggers the event.
        listenerRef.get().onUpdate(new WatchEvent(new EntryEvent(emptyEntry, entry1)));
        assertNotNull(parametersRef.get());
        assertEquals(partId1, parametersRef.get().groupId());

        // Check that the absence of not accepted lease doesn't trigger the event.
        parametersRef.set(null);
        listenerRef.get().onUpdate(new WatchEvent(new EntryEvent(emptyEntry, emptyEntry)));
        assertNull(parametersRef.get());
    }
}
