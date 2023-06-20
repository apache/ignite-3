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

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageChange;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;


/**
 * Utils to manage distribution zones inside tests.
 */
public class DistributionZonesTestUtil {

    /**
     * Creates distribution zone.
     *
     * @param zoneManager Zone manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     * @param dataStorageChangeConsumer Consumer of {@link DataStorageChange}, which sets the right data storage options.
     * @return A future, which will be completed, when create operation finished.
     */
    public static CompletableFuture<Integer> createZone(
            DistributionZoneManager zoneManager,
            String zoneName,
            int partitions,
            int replicas,
            Consumer<DataStorageChange> dataStorageChangeConsumer) {
        var distributionZoneCfgBuilder = new Builder(zoneName)
                .replicas(replicas)
                .partitions(partitions)
                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE);

        if (dataStorageChangeConsumer != null) {
            distributionZoneCfgBuilder
                    .dataStorageChangeConsumer(dataStorageChangeConsumer);
        }

        return zoneManager.createZone(distributionZoneCfgBuilder.build());
    }

    /**
     * Creates distribution zone.
     *
     * @param zoneManager Zone manager.
     * @param zoneName Zone name.
     * @param partitions Zone number of partitions.
     * @param replicas Zone number of replicas.
     * @return A future, which will be completed, when create operation finished.
     */
    public static CompletableFuture<Integer> createZone(
            DistributionZoneManager zoneManager, String zoneName,
            int partitions, int replicas) {
        return createZone(zoneManager, zoneName, partitions, replicas, null);
    }

    /**
     * Alter the number of zone replicas.
     *
     * @param zoneManager Zone manager.
     * @param zoneName Zone name.
     * @param replicas The new number of zone replicas.
     * @return A future, which will be completed, when update operation finished.
     */
    public static CompletableFuture<Void> alterZoneReplicas(DistributionZoneManager zoneManager, String zoneName, int replicas) {
        var distributionZoneCfgBuilder = new Builder(zoneName)
                .replicas(replicas)
                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE);

        return zoneManager.alterZone(zoneName, distributionZoneCfgBuilder.build());
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
                : clusterNodes.stream().map(n -> new Node(n.name(), n.id())).collect(Collectors.toSet());

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
     * Asserts {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZonesChangeTriggerKey(
            long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zonesChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                1000
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
                : clusterNodes.stream().map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes())).collect(Collectors.toSet());

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
    public static void assertLogicalTopologyVersion(long topVer, KeyValueStorage keyValueStorage) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyVersionKey().bytes(),
                ByteUtils::bytesToLong,
                topVer,
                1000
        );
    }

    /**
     * This method is used to initialize the meta storage revision before starting the distribution zone manager.
     * TODO: IGNITE-19403 Watch listeners must be deployed after the zone manager starts.
     *
     * @param metaStorageManager Meta storage manager.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void deployWatchesAndUpdateMetaStorageRevision(MetaStorageManager metaStorageManager) throws InterruptedException {
        // Watches are deployed before distributionZoneManager start in order to update Meta Storage revision before
        // distributionZoneManager's recovery.
        CompletableFuture<Void> deployWatchesFut = metaStorageManager.deployWatches();

        // Bump Meta Storage applied revision by modifying a fake key. DistributionZoneManager breaks on start if Vault is not empty, but
        // Meta Storage revision is equal to 0.
        var fakeKey = new ByteArray("foobar");

        CompletableFuture<Boolean> invokeFuture = deployWatchesFut.thenCompose(unused -> metaStorageManager.invoke(
                Conditions.notExists(fakeKey),
                Operations.put(fakeKey, fakeKey.bytes()),
                Operations.noop()
        ));

        assertThat(invokeFuture, willBe(true));

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() > 0, 10_000));
    }

    /**
     * Sets logical topology to Vault.
     *
     * @param nodes Logical topology
     * @param vaultMgr Vault manager
     */
    public static void mockVaultZonesLogicalTopologyKey(Set<LogicalNode> nodes, VaultManager vaultMgr) {
        Set<NodeWithAttributes> nodesWithAttributes = nodes.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes()))
                .collect(Collectors.toSet());

        byte[] newLogicalTopology = toBytes(nodesWithAttributes);

        assertThat(vaultMgr.put(zonesLogicalTopologyKey(), newLogicalTopology), willCompleteSuccessfully());
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
            int zoneId,
            @Nullable Set<String> expectedValue,
            long timeoutMillis
    ) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-19506 change this to the causality versioned call to dataNodes.
            Set<String> dataNodes = distributionZoneManager.dataNodes(zoneId);

            return Objects.equals(dataNodes, expectedValue);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            Set<String> dataNodes = distributionZoneManager.dataNodes(zoneId);

            assertThat(dataNodes, is(expectedValue));
        }
    }
}
