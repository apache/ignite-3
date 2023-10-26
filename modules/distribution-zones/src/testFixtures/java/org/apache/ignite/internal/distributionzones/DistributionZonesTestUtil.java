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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DataStorageParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterNode;
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
     * @param dataStorage Data storage, {@code null} if not set.
     */
    public static void createZoneWithDataStorage(
            CatalogManager catalogManager,
            String zoneName,
            int partitions,
            int replicas,
            @Nullable String dataStorage
    ) {
        createZone(catalogManager, zoneName, partitions, replicas, null, null, null, dataStorage);
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
        createZone(catalogManager, zoneName, partitions, replicas, null, null, null, null);
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
        createZone(catalogManager, zoneName, null, null, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown, filter, null);
    }

    private static void createZone(
            CatalogManager catalogManager,
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable String dataStorage
    ) {
        CreateZoneParams.Builder builder = CreateZoneParams.builder().zoneName(zoneName);

        if (partitions != null) {
            builder.partitions(partitions);
        }

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

        if (dataStorage != null) {
            builder.dataStorage(DataStorageParams.builder().engine(dataStorage).build());
        }

        assertThat(catalogManager.createZone(builder.build()), willCompleteSuccessfully());
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesKey(int)}.
     *
     * @param zoneId Zone id.
     * @param clusterNodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesForZone(
            int zoneId,
            @Nullable Set<LogicalNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Set<Node> nodes = clusterNodes == null
                ? null
                : clusterNodes.stream().map(n -> new Node(n.name(), n.id())).collect(toSet());

        assertValueInStorage(
                keyValueStorage,
                zoneDataNodesKey(zoneId).bytes(),
                value -> DistributionZonesUtil.dataNodes(fromBytes(value)),
                nodes,
                2000
        );
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesKey(int)}.
     *
     * @param zoneId Zone id.
     * @param nodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesForZoneWithAttributes(
            int zoneId,
            @Nullable Set<Node> nodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertDataNodesForZoneWithAttributes(zoneId, nodes, keyValueStorage, DEFAULT_FILTER);
    }

    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesKey(int)} and with provided {@code filter}.
     *
     * @param zoneId Zone id.
     * @param nodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @param filter Filter for data nodes.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesForZoneWithAttributes(
            int zoneId,
            @Nullable Set<Node> nodes,
            KeyValueStorage keyValueStorage,
            String filter
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneDataNodesKey(zoneId).bytes(),
                value -> DistributionZonesUtil.dataNodes(fromBytes(value)),
                nodes,
                2000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZoneScaleUpChangeTriggerKey(
            @Nullable Long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneScaleUpChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                2000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZoneScaleDownChangeTriggerKey(
            @Nullable Long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneScaleDownChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                2000
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
                : clusterNodes.stream().map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes())).collect(toSet());

        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyKey().bytes(),
                ByteUtils::fromBytes,
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
                ByteUtils::bytesToLong,
                topVer,
                1000
        );
    }

    /**
     * Sets logical topology to Vault.
     *
     * @param nodes Logical topology
     * @param metaStorageManager Meta storage manager
     */
    public static void mockZonesLogicalTopologyAndAttributes(
            Set<LogicalNode> nodes,
            MetaStorageManager metaStorageManager
    ) {
        Set<NodeWithAttributes> nodesWithAttributes = nodes.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.userAttributes()))
                .collect(toSet());

        byte[] newLogicalTopology = toBytes(nodesWithAttributes);

        Map<String, Map<String, String>> nodesAttributes = new ConcurrentHashMap<>();
        nodesWithAttributes.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));
        assertThat(metaStorageManager.put(zonesNodesAttributes(), toBytes(nodesAttributes)), willCompleteSuccessfully());

        assertThat(metaStorageManager.put(zonesLogicalTopologyKey(), newLogicalTopology), willCompleteSuccessfully());
        assertThat(metaStorageManager.put(zonesLogicalTopologyVersionKey(), longToBytes(1L)), willCompleteSuccessfully());
    }

    /**
     * Sets logical topology to Meta Storage.
     *
     * @param nodes Logical topology
     * @param topVer Topology version
     * @param metaStorageManager Meta Storage manager.
     */
    public static void setLogicalTopologyInMetaStorage(Set<LogicalNode> nodes, long topVer, MetaStorageManager metaStorageManager) {
        Iif iff = iif(
                Conditions.exists(zonesLogicalTopologyKey()),
                updateLogicalTopologyAndVersion(nodes, topVer),
                ops().yield(false)
        );

        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(iff).thenApply(StatementResult::getAsBoolean);

        assertThat(invokeFuture, willBe(true));
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
            int zoneId,
            @Nullable Set<LogicalNode> expectedValue,
            long timeoutMillis
    ) throws InterruptedException, ExecutionException, TimeoutException {
        Set<String> expectedValueNames =
                expectedValue == null ? null : expectedValue.stream().map(ClusterNode::name).collect(toSet());

        boolean success = waitForCondition(() -> {
            Set<String> dataNodes = null;
            try {
                dataNodes = distributionZoneManager.dataNodes(causalityToken.get(), zoneId).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                // Ignore
            }

            return Objects.equals(dataNodes, expectedValueNames);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            Set<String> dataNodes = distributionZoneManager.dataNodes(causalityToken.get(), zoneId).get(5, TimeUnit.SECONDS);

            assertThat(dataNodes, is(expectedValueNames));
        }
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
        AlterZoneParams.Builder builder = AlterZoneParams.builder().zoneName(zoneName);

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

        assertThat(catalogManager.alterZone(builder.build()), willCompleteSuccessfully());
    }

    /**
     * Drops a distribution zone from the catalog.
     *
     * @param catalogManager Catalog manager.
     * @param zoneName Zone name.
     */
    public static void dropZone(CatalogManager catalogManager, String zoneName) {
        assertThat(catalogManager.dropZone(DropZoneParams.builder().zoneName(zoneName).build()), willCompleteSuccessfully());
    }

    /**
     * Returns distributed zone by ID from catalog, {@code null} if zone is absent.
     *
     * @param catalogService Catalog service.
     * @param zoneId Zone ID.
     * @param timestamp Timestamp.
     */
    public static @Nullable CatalogZoneDescriptor getZoneById(CatalogService catalogService, int zoneId, long timestamp) {
        return catalogService.zone(zoneId, timestamp);
    }

    /**
     * Returns distributed zone ID form catalog, {@code null} if zone is absent.
     *
     * @param catalogService Catalog service.
     * @param zoneName Distributed zone name.
     * @param timestamp Timestamp.
     */
    public static @Nullable Integer getZoneId(CatalogService catalogService, String zoneName, long timestamp) {
        CatalogZoneDescriptor zone = catalogService.zone(zoneName, timestamp);

        return zone == null ? null : zone.id();
    }

    /**
     * Returns distributed zone ID form catalog.
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
}
