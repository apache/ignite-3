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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.TestDefaultProfilesNames;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorage;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;

/**
 * Set of tests that are checking how in-memory scenarios are working under zone colocation.
 */
public class ItZoneInMemoryTest extends ItAbstractColocationTest {
    @Test
    void testMixedStorageProfilesZone() throws Exception {
        startCluster(1);

        Node node = getNode(0);

        String zoneName = "mixed_zone";

        int zoneId = createZoneWithStorageProfiles(
                node,
                zoneName,
                1,
                1,
                TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME,
                TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME
        );

        checkZoneConsistsOfMixedStorageProfiles(node, zoneId);

        assertFalse(isRaftLogStorageVolatile(node, zoneId));

        checkWriteReadInVolatileTableStorageIsSuccessful(node, zoneName);
    }

    @Test
    void testVolatileStorageProfileZone() throws Exception {
        startCluster(1);

        Node node = getNode(0);

        String zoneName = "mixed_zone";
        int zoneId = createZoneWithStorageProfiles(
                node,
                zoneName,
                1,
                1,
                TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME,
                TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME
        );

        checkZoneConsistsOfVolatileOnlyStorageProfile(node, zoneId);

        assertTrue(isRaftLogStorageVolatile(node, zoneId));

        checkWriteReadInVolatileTableStorageIsSuccessful(node, zoneName);
    }

    private static void checkWriteReadInVolatileTableStorageIsSuccessful(Node node, String zoneName) throws NodeStoppingException {
        String tableName = "volatile_table";

        createTable(node, zoneName, tableName, TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME);
        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());

        checkTableStorageIsVolatile(node, tableId);

        KeyValueView<Long, Integer> kvView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

        Map<Long, Integer> values = Map.of(
                0L, 0,
                1L, 1,
                2L, 2
        );

        node.transactions().runInTransaction(tx -> {
            assertDoesNotThrow(() -> kvView.putAll(tx, values));
        });

        node.transactions().runInTransaction(tx -> {
            assertEquals(values.size(), assertDoesNotThrow(() -> kvView.getAll(tx, Set.of(0L, 1L, 2L))).size());
        });
    }

    private static void checkTableStorageIsVolatile(Node node, int tableId) throws NodeStoppingException {
        InternalTableImpl internalTable = (InternalTableImpl) node.tableManager.table(tableId).internalTable();

        assertInstanceOf(VolatilePageMemoryTableStorage.class, internalTable.storage());
    }

    private static List<CatalogStorageProfileDescriptor> extractZoneProfiles(Node node, int zoneId) {
        CatalogZoneDescriptor zoneDescriptor = node.catalogManager.latestCatalog().zone(zoneId);

        assertNotNull(zoneDescriptor);

        return zoneDescriptor.storageProfiles().profiles();
    }

    private static String logStorageProfilesStatusMessage(Node node, List<CatalogStorageProfileDescriptor> storageProfilesDescriptors) {
        return IgniteStringFormatter.format("Storage engines that are present: {}", storageProfilesDescriptors.stream()
                .map(storageProfilesDescriptor -> {
                    String storageProfile = storageProfilesDescriptor.storageProfile();

                    StorageEngine storageEngine = node.dataStorageManager().engineByStorageProfile(storageProfile);

                    if (storageEngine == null) {
                        return storageProfile + ": null";
                    } else if (storageEngine.isVolatile()) {
                        return storageProfile + ": volatile";
                    } else {
                        return storageProfile + ": persistent";
                    }
                }).collect(Collectors.toList()));
    }

    private static boolean isRaftLogStorageVolatile(Node node, int zoneId) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, 0);

        String singlePeerConsistentId = node.clusterService.topologyService().localMember().name();

        RaftNodeId raftNodeId = new RaftNodeId(zonePartitionId, new Peer(singlePeerConsistentId));

        JraftServerImpl raftServer = (JraftServerImpl) node.raftManager.server();

        RaftGroupService raftService = raftServer.raftGroupService(raftNodeId);

        NodeImpl raftNode = (NodeImpl) raftService.getRaftNode();

        LogStorage raftLogStorage = raftNode.logStorage();

        return raftLogStorage instanceof VolatileLogStorage;
    }

    private static void checkZoneConsistsOfMixedStorageProfiles(Node node, int zoneId) {
        List<CatalogStorageProfileDescriptor> zoneProfiles = extractZoneProfiles(node, zoneId);

        assertEquals(2, zoneProfiles.size());

        boolean volatileEngineIsPresent = false;
        boolean persistentEngineIsPresent = false;

        for (CatalogStorageProfileDescriptor profile : zoneProfiles) {
            StorageEngine engine = node.dataStorageManager().engineByStorageProfile(profile.storageProfile());

            assertNotNull(engine);

            if (engine.isVolatile()) {
                volatileEngineIsPresent = true;
            } else {
                persistentEngineIsPresent = true;
            }
        }

        String assertionMessage = logStorageProfilesStatusMessage(node, zoneProfiles);

        assertTrue(volatileEngineIsPresent, assertionMessage);
        assertTrue(persistentEngineIsPresent, assertionMessage);
    }

    private static void checkZoneConsistsOfVolatileOnlyStorageProfile(Node node, int zoneId) {
        List<CatalogStorageProfileDescriptor> zoneProfiles = extractZoneProfiles(node, zoneId);

        assertEquals(2, zoneProfiles.size());

        boolean areAllEnginesVolatile = zoneProfiles.stream().allMatch(profileDesc -> {
            StorageEngine storageEngine = node.dataStorageManager().engineByStorageProfile(profileDesc.storageProfile());

            return storageEngine != null && storageEngine.isVolatile();
        });

        assertTrue(areAllEnginesVolatile, logStorageProfilesStatusMessage(node, zoneProfiles));
    }
}
