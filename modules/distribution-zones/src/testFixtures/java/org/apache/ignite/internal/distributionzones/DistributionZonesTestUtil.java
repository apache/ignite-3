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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_FILTER;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneVersionedConfigurationKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVault;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributesVault;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.causalitydatanodes.CausalityDataNodesEngine.ZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageChange;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
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
    public static void mockVaultZonesLogicalTopologyKey(Set<LogicalNode> nodes, VaultManager vaultMgr, long appliedRevision) {
        Set<NodeWithAttributes> nodesWithAttributes = nodes.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.id(), n.nodeAttributes()))
                .collect(Collectors.toSet());

        byte[] newLogicalTopology = toBytes(nodesWithAttributes);

        Map<String, Map<String, String>> nodesAttributes = new ConcurrentHashMap<>();
        nodesWithAttributes.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));
        assertThat(vaultMgr.put(zonesNodesAttributesVault(), toBytes(nodesAttributes)), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesLogicalTopologyVault(), newLogicalTopology), willCompleteSuccessfully());

        assertThat(vaultMgr.put(zonesGlobalStateRevision(), longToBytes(appliedRevision)), willCompleteSuccessfully());

        ConcurrentSkipListMap<Long, ZoneConfiguration> map = new ConcurrentSkipListMap<>();

        map.put(1L, new ZoneConfiguration(false, 2, 3, "asd"));

        assertThat(vaultMgr.put(zoneVersionedConfigurationKey(123456789), toBytes(map)), willCompleteSuccessfully());

        System.out.println();
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
            @Nullable Set<LogicalNode> expectedValue,
            long timeoutMillis
    ) throws InterruptedException {
        Set<String> expectedValueNames =
                expectedValue == null ? null : expectedValue.stream().map(ClusterNode::name).collect(Collectors.toSet());

        boolean success = waitForCondition(() -> {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-19506 change this to the causality versioned call to dataNodes.
            Set<String> dataNodes = distributionZoneManager.dataNodes(zoneId);

            return Objects.equals(dataNodes, expectedValueNames);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            Set<String> dataNodes = distributionZoneManager.dataNodes(zoneId);

            assertThat(dataNodes, is(expectedValueNames));
        }
    }

    /**
     * Changes a filter value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param filter New filter value.
     * @return Revision.
     * @throws Exception If failed.
     */
    public static long alterFilterAndGetRevision(
            String zoneName,
            String filter,
            DistributionZoneManager distributionZoneManager,
            ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneChangeFilterRevisions
    ) throws Exception {
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneChangeFilterRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .filter(filter).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneChangeFilterRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    public static ConfigurationListener<String> onUpdateFilter(
            ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneChangeFilterRevisions
    ) {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneChangeFilterRevisions.containsKey(zoneId)) {
                zoneChangeFilterRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * Changes a scale down timer value of a zone and return the revision of a zone update event.
     *
     * @param zoneName Zone name.
     * @param scaleDown New scale down value.
     * @return Revision.
     * @throws Exception If failed.
     */
    public static long alterZoneScaleUpAndDownAndGetRevision(
            String zoneName,
            int scaleUp,
            int scaleDown,
            DistributionZoneManager distributionZoneManager,
            ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleUpRevisions,
            ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions) throws Exception {
        CompletableFuture<Long> scaleUpRevisionFut = new CompletableFuture<>();
        CompletableFuture<Long> scaleDownRevisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleUpRevisions.put(zoneId, scaleUpRevisionFut);
        zoneScaleDownRevisions.put(zoneId, scaleDownRevisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(scaleUp)
                        .dataNodesAutoAdjustScaleDown(scaleDown)
                        .build())
                .get(3, SECONDS);

        long scaleUpRevision = scaleUpRevisionFut.get(3, SECONDS);
        long scaleDownRevision = scaleDownRevisionFut.get(3, SECONDS);

        assertEquals(scaleUpRevision, scaleDownRevision);

        return scaleUpRevision;
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleUpRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    public static ConfigurationListener<Integer> onUpdateScaleUp(ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleUpRevisions) {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleUpRevisions.containsKey(zoneId)) {
                zoneScaleUpRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    /**
     * Creates a configuration listener which completes futures from {@code zoneScaleDownRevisions}
     * when receives event with expected zone id.
     *
     * @return Configuration listener.
     */
    public static ConfigurationListener<Integer> onUpdateScaleDown(ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions) {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleDownRevisions.containsKey(zoneId)) {
                zoneScaleDownRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }



    /**
     * Puts a given node as a part of the logical topology and return revision of a topology watch listener event.
     *
     * @param node Node to put.
     * @param expectedTopology Expected topology for future completing.
     * @return Revision.
     * @throws Exception If failed.
     */
    public static long putNodeInLogicalTopologyAndGetRevision(
            LogicalNode node,
            Set<LogicalNode> expectedTopology,
            LogicalTopology topology,
            ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions

    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(ClusterNode::name).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.putNode(node);

        return revisionFut.get(5, SECONDS);
    }

    /**
     * Creates a topology watch listener which completes futures from {@code topologyRevisions}
     * when receives event with expected logical topology.
     *
     * @return Watch listener.
     */
    public static WatchListener createMetastorageTopologyListener(
            ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions
    ) {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                Set<NodeWithAttributes> newLogicalTopology = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                        revision = e.revision();
                    } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopology = fromBytes(e.value());
                    }
                }

                Set<String> nodeNames = newLogicalTopology.stream().map(node -> node.nodeName()).collect(toSet());

                System.out.println("MetastorageTopologyListener test " + nodeNames);

                if (topologyRevisions.containsKey(nodeNames)) {
                    topologyRevisions.remove(nodeNames).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }
}
