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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_QUORUM_SIZE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodeHistoryContextFromValues;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DataNodesHistoryContext;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Utils to manage distribution zones inside tests.
 */
public class DistributionZonesTestUtil {
    /**
     * Creates distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     * @param storageProfile Storage profile.
     */
    public static void createZoneWithStorageProfile(
            CatalogManager catalogManager,
            String zoneName,
            int partitions,
            int replicas,
            String storageProfile
    ) {
        createZone(catalogManager, zoneName, partitions, replicas, null, null, null, null, null, storageProfile);
    }

    /**
     * Creates distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     */
    public static void createZone(CatalogManager catalogManager, String zoneName, int partitions, int replicas) {
        createZone(catalogManager, zoneName, partitions, replicas, null, null, null, null, null,  DEFAULT_STORAGE_PROFILE);
    }

    /**
     * Creates a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param consistencyMode Zone consistency mode.
     */
    public static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            ConsistencyMode consistencyMode
    ) {
        createZone(
                catalogManager,
                zoneName,
                null,
                null,
                null,
                null,
                null,
                null,
                consistencyMode,
                DEFAULT_STORAGE_PROFILE
        );
    }

    /**
     * Creates a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param filter Nodes filter, {@code null} if not set.
     */
    public static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        createZone(
                catalogManager,
                zoneName,
                null,
                null,
                null,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                null,
                DEFAULT_STORAGE_PROFILE
        );
    }

    /**
     * Creates a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param filter Nodes filter, {@code null} if not set.
     * @param storageProfiles Storage profiles.
     */
    public static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            String storageProfiles
    ) {
        createZone(
                catalogManager,
                zoneName,
                null,
                null,
                null,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                null,
                storageProfiles
        );
    }

    /**
     * Creates a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param filter Nodes filter, {@code null} if not set.
     * @param consistencyMode Consistency mode, {@code null} if not set..
     * @param storageProfiles Storage profiles.
     */
    public static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable ConsistencyMode consistencyMode,
            String storageProfiles
    ) {
        createZone(
                catalogManager,
                zoneName,
                null,
                null,
                null,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                consistencyMode,
                storageProfiles
        );
    }

    /**
     * Creates a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
     * @param consistencyMode Zone consistency mode.
     */
    public static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            Integer partitions,
            Integer replicas,
            Integer dataNodesAutoAdjustScaleUp,
            Integer dataNodesAutoAdjustScaleDown,
            ConsistencyMode consistencyMode
    ) {
        createZone(
                catalogManager,
                zoneName,
                partitions,
                replicas,
                null,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                null,
                consistencyMode,
                DEFAULT_STORAGE_PROFILE
        );
    }

    private static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer quorumSize,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable ConsistencyMode consistencyMode,
            String storageProfiles
    ) {
        CreateZoneCommandBuilder builder = CreateZoneCommand.builder().zoneName(zoneName);

        if (partitions != null) {
            builder.partitions(partitions);
        }

        if (replicas != null) {
            builder.replicas(replicas);
        }

        if (quorumSize != null) {
            builder.quorumSize(quorumSize);
        }

        if (dataNodesAutoAdjustScaleUp != null) {
            builder.dataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp);
        }

        if (dataNodesAutoAdjustScaleDown != null) {
            builder.dataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown);
        }

        if (filter != null) {
            builder.filter(filter);
        }

        if (consistencyMode != null) {
            builder.consistencyModeParams(consistencyMode);
        }

        assertNotNull(storageProfiles);

        builder.storageProfilesParams(parseStorageProfiles(storageProfiles));

        assertThat(catalogManager.execute(builder.build()), willCompleteSuccessfully());
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesHistoryKey(int)} in storage with set of LogicalNodes as input.
     *
     * @param zoneId Zone id.
     * @param clusterNodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesFromLogicalNodesInStorage(
            int zoneId,
            @Nullable Set<LogicalNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Set<Node> nodes = clusterNodes == null
                ? null
                : clusterNodes.stream().map(n -> new Node(n.name(), n.id())).collect(toSet());

        assertDataNodesInStorage(
                zoneId,
                nodes,
                keyValueStorage
        );
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesHistoryKey(int)} in storage.
     *
     * @param zoneId Zone id.
     * @param nodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesInStorage(
            int zoneId,
            @Nullable Set<Node> nodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertDataNodesInStorage(zoneId, nodes, HybridTimestamp.MAX_VALUE, keyValueStorage);
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesHistoryKey(int)} in storage.
     *
     * @param zoneId Zone id.
     * @param nodes Data nodes.
     * @param timestamp Timestamp.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesInStorage(
            int zoneId,
            @Nullable Set<Node> nodes,
            HybridTimestamp timestamp,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        byte[] key = zoneDataNodesHistoryKey(zoneId).bytes();

        HybridTimestamp timestampToCheck = timestamp == HybridTimestamp.MAX_VALUE ? timestamp : timestamp.tick();

        Supplier<Set<Node>> nodesGetter = () -> {
            byte[] storageValue = keyValueStorage.get(key).value();

            if (storageValue == null) {
                return null;
            }

            DataNodesHistory history = DataNodesHistorySerializer.deserialize(storageValue);

            DataNodesHistoryEntry dataNodes = history.dataNodesForTimestamp(timestampToCheck);

            return dataNodes.dataNodes()
                    .stream()
                    .map(n -> new Node(n.nodeName(), n.nodeId()))
                    .collect(toSet());
        };

        boolean success = waitForCondition(() -> {
            Set<Node> actualNodes = nodesGetter.get();

            return Objects.equals(actualNodes, nodes);
        }, SECONDS.toMillis(2000));

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            Set<Node> actualNodes = nodesGetter.get();
            Set<String> actualNodeNames = actualNodes == null ? emptySet() : actualNodes.stream().map(Node::nodeName).collect(toSet());

            assertEquals(nodes, actualNodes, "Nodes: " + actualNodeNames
                    + ", timestamp=" + (timestampToCheck == HybridTimestamp.MAX_VALUE ? "[max]" : timestamp));
        }
    }

    /**
     * Creates a mock {@link LogicalNode} from a {@link Node}.
     *
     * @param node Node.
     * @return Logical node.
     */
    public static LogicalNode logicalNodeFromNode(Node node) {
        return new LogicalNode(
                new ClusterNodeImpl(node.nodeId(), node.nodeName(), new NetworkAddress("localhost", 123)),
                emptyMap(),
                emptyMap(),
                List.of("default")
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyKey()} value.
     *
     * @param clusterNodes Expected cluster nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopology(
            @Nullable Set<LogicalNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Set<NodeWithAttributes> nodes = clusterNodes == null
                ? null
                : clusterNodes.stream().map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                        .collect(toSet());

        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyKey().bytes(),
                DistributionZonesUtil::deserializeLogicalTopologySet,
                nodes,
                1000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyKey()} value in Meta Storage.
     *
     * @param clusterNodes Expected cluster nodes.
     * @param metaStorageManager Meta Storage manager.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopologyInMetastorage(
            @Nullable Set<LogicalNode> clusterNodes,
            MetaStorageManager metaStorageManager
    ) throws InterruptedException {
        Set<NodeWithAttributes> nodes = clusterNodes == null
                ? null
                : clusterNodes.stream()
                        .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes(), n.storageProfiles()))
                        .collect(toSet());

        assertValueInStorage(
                metaStorageManager,
                zonesLogicalTopologyKey(),
                DistributionZonesUtil::deserializeLogicalTopologySet,
                nodes,
                1000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyVersionKey()} value.
     *
     * @param topVer Topology version.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopologyVersion(@Nullable Long topVer, KeyValueStorage keyValueStorage) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyVersionKey().bytes(),
                ByteUtils::bytesToLongKeepingOrder,
                topVer,
                1000
        );
    }

    /**
     * Asserts value from the storage.
     *
     * @param keyValueStorage Key-value storage.
     * @param key Key of value to check.
     * @param valueTransformer Function that is applied to value from the storage.
     * @param expectedValue Expected value.
     * @param timeoutMillis Timeout in milliseconds.
     * @param <T> A type of value from storage.
     * @throws InterruptedException If interrupted.
     */
    public static <T> void assertValueInStorage(
            KeyValueStorage keyValueStorage,
            byte[] key,
            Function<byte[], T> valueTransformer,
            @Nullable T expectedValue,
            long timeoutMillis
    ) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            byte[] storageValue = keyValueStorage.get(key).value();

            T actualValue = storageValue == null ? null : valueTransformer.apply(storageValue);

            return Objects.equals(actualValue, expectedValue);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            byte[] storageValue = keyValueStorage.get(key).value();

            assertThat(storageValue == null ? null : valueTransformer.apply(storageValue), is(expectedValue));
        }
    }

    /**
     * Asserts value from the meta storage.
     *
     * @param metaStorageManager Meta Storage manager.
     * @param key Key of value to check.
     * @param valueTransformer Function that is applied to value from the meta storage.
     * @param expectedValue Expected value.
     * @param timeoutMillis Timeout in milliseconds.
     * @param <T> A type of value from the meta storage.
     * @throws InterruptedException If interrupted.
     */
    public static <T> void assertValueInStorage(
            MetaStorageManager metaStorageManager,
            ByteArray key,
            Function<byte[], T> valueTransformer,
            @Nullable T expectedValue,
            long timeoutMillis
    ) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            byte[] storageValue = new byte[0];
            try {
                storageValue = metaStorageManager.get(key).get().value();
            } catch (InterruptedException | ExecutionException e) {
                fail();
            }

            T actualValue = storageValue == null ? null : valueTransformer.apply(storageValue);

            return Objects.equals(actualValue, expectedValue);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            byte[] storageValue = new byte[0];

            try {
                storageValue = metaStorageManager.get(key).get().value();
            } catch (ExecutionException e) {
                fail();
            }

            assertThat(storageValue == null ? null : valueTransformer.apply(storageValue), is(expectedValue));
        }
    }

    /**
     * Asserts data nodes from the distribution zone manager.
     *
     * @param distributionZoneManager Distribution zone manager.
     * @param zoneId Zone id.
     * @param expectedValue Expected value.
     * @param timeoutMillis Timeout in milliseconds.
     * @throws InterruptedException If interrupted.
     */
    public static void assertDataNodesFromManager(
            DistributionZoneManager distributionZoneManager,
            Supplier<Long> causalityToken,
            Supplier<Integer> catalogVersion,
            int zoneId,
            @Nullable Set<LogicalNode> expectedValue,
            long timeoutMillis
    ) throws InterruptedException, ExecutionException, TimeoutException {
        Set<String> expectedValueNames =
                expectedValue == null ? null : expectedValue.stream().map(InternalClusterNode::name).collect(toSet());

        boolean success = waitForCondition(() -> {
            Set<String> dataNodes = null;
            try {
                dataNodes = distributionZoneManager.dataNodesManager()
                        .dataNodes(zoneId, HybridTimestamp.MAX_VALUE).get(5, SECONDS);
            } catch (Exception e) {
                // Ignore
            }

            return Objects.equals(dataNodes, expectedValueNames);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            Set<String> dataNodes = distributionZoneManager.dataNodesManager()
                    .dataNodes(zoneId, HybridTimestamp.MAX_VALUE).get(5, SECONDS);

            assertThat(dataNodes, is(expectedValueNames));
        }
    }

    /**
     * Deserialize the latest data nodes history entry from the given history in serialized format.
     *
     * @param bytes Serialized {@link DataNodesHistory}.
     * @return Latest entry.
     */
    public static Set<NodeWithAttributes> deserializeLatestDataNodesHistoryEntry(byte[] bytes) {
        return requireNonNull(parseDataNodes(bytes, HybridTimestamp.MAX_VALUE));
    }

    /**
     * Get {@link DataNodesHistoryContext} from meta storage.
     *
     * @param metaStorageManager Meta storage manager.
     * @param zoneId Zone id.
     * @return Data node history context.
     */
    public static DataNodesHistoryContext dataNodeHistoryContext(MetaStorageManager metaStorageManager, int zoneId) {
        CompletableFuture<Map<ByteArray, Entry>> fut = metaStorageManager.getAll(Set.of(
                zoneDataNodesHistoryKey(zoneId),
                zoneScaleUpTimerKey(zoneId),
                zoneScaleDownTimerKey(zoneId)
        ));

        assertThat(fut, willCompleteSuccessfully());

        return dataNodeHistoryContextFromValues(fut.join().values());
    }

    /**
     * Alters a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param dataNodesAutoAdjustScaleUp Timeout in seconds between node added topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch,
     *         {@code null} if not set.
     * @param filter Nodes filter, {@code null} if not set.
     */
    public static void alterZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        alterZone(catalogManager, zoneName, null, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown, filter);
    }

    /**
     * Alters a distribution zone in the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     * @param replicas New number of zone replicas.
     */
    public static void alterZone(CatalogManager catalogManager, String zoneName, int replicas) {
        alterZone(catalogManager, zoneName, replicas, null, null, null);
    }

    private static void alterZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        AlterZoneCommandBuilder builder = AlterZoneCommand.builder().zoneName(zoneName);

        if (replicas != null) {
            builder.replicas(replicas);
        }

        if (dataNodesAutoAdjustScaleUp != null) {
            builder.dataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp);
        }

        if (dataNodesAutoAdjustScaleDown != null) {
            builder.dataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown);
        }

        if (filter != null) {
            builder.filter(filter);
        }

        assertThat(catalogManager.execute(builder.build()), willCompleteSuccessfully());
    }

    /**
     * Drops a distribution zone from the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     */
    public static void dropZone(CatalogManager catalogManager, String zoneName) {
        CatalogCommand dropCommand = DropZoneCommand.builder()
                .zoneName(zoneName)
                .build();
        assertThat(catalogManager.execute(dropCommand), willCompleteSuccessfully());
    }

    /**
     * Returns distributed zone ID from catalog, {@code null} if zone is absent.
     *
     * @param catalogService Catalog service.
     * @param zoneName Distributed zone name.
     * @param timestamp Timestamp.
     */
    public static @Nullable Integer getZoneId(CatalogService catalogService, String zoneName, long timestamp) {
        CatalogZoneDescriptor zone = catalogService.activeCatalog(timestamp).zone(zoneName);

        return zone == null ? null : zone.id();
    }

    /**
     * Creates a zone with default parameters and makes it default zone.
     *
     * @param catalogManager Catalog manager.
     */
    public static void createDefaultZone(CatalogManager catalogManager) {
        createZone(
                catalogManager,
                DEFAULT_ZONE_NAME,
                DEFAULT_PARTITION_COUNT,
                DEFAULT_REPLICA_COUNT,
                DEFAULT_ZONE_QUORUM_SIZE,
                IMMEDIATE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                DEFAULT_FILTER,
                STRONG_CONSISTENCY,
                DEFAULT_STORAGE_PROFILE
        );

        setDefaultZone(catalogManager, DEFAULT_ZONE_NAME);

        Catalog latestCatalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        assertNotNull(latestCatalog.defaultZone());
    }

    /**
     * Alters a zone with the given name default.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     */
    public static void setDefaultZone(CatalogManager catalogManager, String zoneName) {
        CatalogCommand command = AlterZoneSetDefaultCommand.builder()
                .zoneName(zoneName)
                .ifExists(true)
                .build();

        assertThat(catalogManager.execute(command), willCompleteSuccessfully());
    }

    /** Returns default distribution zone. */
    public static CatalogZoneDescriptor getDefaultZone(CatalogService catalogService, long timestamp) {
        Catalog catalog = catalogService.activeCatalog(timestamp);

        requireNonNull(catalog);

        return requireNonNull(catalog.defaultZone());
    }

    /**
     * Returns distributed zone ID from catalog.
     *
     * @param catalogService Catalog service.
     * @param zoneName Distributed zone name.
     * @param timestamp Timestamp.
     * @throws AssertionError If zone is absent.
     */
    public static int getZoneIdStrict(CatalogService catalogService, String zoneName, long timestamp) {
        Integer zoneId = getZoneId(catalogService, zoneName, timestamp);

        assertNotNull(zoneId, "zoneName=" + zoneName + ", timestamp=" + timestamp);

        return zoneId;
    }

    /**
     * Returns stable partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Stable partition assignments key.
     */
    public static ByteArray stablePartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (colocationEnabled()) {
            return ZoneRebalanceUtil.stablePartAssignmentsKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.stablePartAssignmentsKey((TablePartitionId) partitionGroupId);
        }
    }

    /**
     * Returns pending partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Pending partition assignments key.
     */
    public static ByteArray pendingPartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (colocationEnabled()) {
            return ZoneRebalanceUtil.pendingPartAssignmentsQueueKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.pendingPartAssignmentsQueueKey((TablePartitionId) partitionGroupId);
        }
    }

    /**
     * Returns planned partition assignments key.
     *
     * @param partitionGroupId Partition group identifier.
     * @return Planned partition assignments key.
     */
    public static ByteArray plannedPartitionAssignmentsKey(PartitionGroupId partitionGroupId) {
        if (colocationEnabled()) {
            return ZoneRebalanceUtil.plannedPartAssignmentsKey((ZonePartitionId) partitionGroupId);
        } else {
            return RebalanceUtil.plannedPartAssignmentsKey((TablePartitionId) partitionGroupId);
        }
    }
}
