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

import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Util class for methods for Distribution zones tests.
 */
public class DistributionZonesTestUtil {
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
            @Nullable Set<String> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Assertions.assertTrue(IgniteTestUtils.waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<String> res = DistributionZonesUtil.dataNodes(ByteUtils.fromBytes(dataNodes));

                    return res.equals(clusterNodes);
                },
                2000
        ));
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
            int revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Assertions.assertTrue(
                IgniteTestUtils.waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zoneScaleUpChangeTriggerKey(zoneId).bytes()).value()) == revision,
                        2000
                )
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
            int revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Assertions.assertTrue(
                IgniteTestUtils.waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zoneScaleDownChangeTriggerKey(zoneId).bytes()).value()) == revision,
                        2000
                )
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
            int revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Assertions.assertTrue(
                IgniteTestUtils.waitForCondition(
                        () -> ByteUtils.bytesToLong(keyValueStorage.get(zonesChangeTriggerKey(zoneId).bytes()).value()) == revision, 1000
                )
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
            @Nullable Set<ClusterNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : ByteUtils.toBytes(clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet()));

        Assertions.assertTrue(
                IgniteTestUtils.waitForCondition(() -> Arrays.equals(keyValueStorage.get(zonesLogicalTopologyKey().bytes()).value(), nodes), 1000));
    }

    /**
     * Delegates meta storage manager commands directly to meta storage listener.
     *
     * @param raftIndex Raft index.
     * @param metaStorageListener Meta storage listener.
     * @param metaStorageService Meta storage service.
     * @param metaStorageManager Meta storage manager.
     */
    public static void mockMetaStorageListener(
            AtomicLong raftIndex,
            MetaStorageListener metaStorageListener,
            RaftGroupService metaStorageService,
            MetaStorageManager metaStorageManager) {
        // Delegate directly to listener.
        Mockito.lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public WriteCommand command() {
                            return (WriteCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(ArgumentMatchers.any(WriteCommand.class));

        Mockito.lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CommandClosure<ReadCommand> clo = new CommandClosure<>() {
                        /** {@inheritDoc} */
                        @Override
                        public long index() {
                            return commandIndex;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public ReadCommand command() {
                            return (ReadCommand) cmd;
                        }

                        /** {@inheritDoc} */
                        @Override
                        public void result(@Nullable Serializable r) {
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        metaStorageListener.onRead(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new IgniteInternalException(e));
                    }

                    return res;
                }
        ).when(metaStorageService).run(ArgumentMatchers.any(ReadCommand.class));

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        Mockito.lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(ArgumentMatchers.any());

        Mockito.lenient().doAnswer(invocationClose -> {
            Set<ByteArray> keysSet = invocationClose.getArgument(0);

            GetAllCommand getAllCommand = commandsFactory.getAllCommand().keys(
                    keysSet.stream().map(ByteArray::bytes).collect(Collectors.toList())
            ).revision(0).build();

            return metaStorageService.<List<Entry>>run(getAllCommand).thenApply(entries -> {
                Map<ByteArray, Entry> res = new HashMap<>();

                for (Entry e : entries) {
                    ByteArray key = new ByteArray(e.key());

                    res.put(key, new EntryImpl(key.bytes(), e.value(), e.revision(), e.updateCounter()));
                }

                return res;
            });
        }).when(metaStorageManager).getAll(ArgumentMatchers.any());

        Mockito.lenient().doAnswer(invocationClose -> {
            ByteArray key = invocationClose.getArgument(0);

            GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

            return metaStorageService.<Entry>run(getCommand).thenApply(
                    entry -> new EntryImpl(entry.key(), entry.value(), entry.revision(), entry.updateCounter())
            );
        }).when(metaStorageManager).get(ArgumentMatchers.any());
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
    public static CompletableFuture<Integer> createZone(DistributionZoneManager zoneManager, String zoneName, int partitions, int replicas) {
        var distributionZoneCfgBuilder = new DistributionZoneConfigurationParameters.Builder(zoneName)
                .replicas(replicas)
                .partitions(partitions);
        var distributionZoneCfg = distributionZoneCfgBuilder.build();

        return zoneManager.createZone(distributionZoneCfg).thenApply((v) -> zoneManager.getZoneId(zoneName));
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
        var distributionZoneCfgBuilder = new DistributionZoneConfigurationParameters.Builder(zoneName)
                  .replicas(replicas);

        return zoneManager.alterZone(zoneName, distributionZoneCfgBuilder.build());
    }
}
