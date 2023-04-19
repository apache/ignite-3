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

package org.apache.ignite.internal.distributionzones.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.HybridTimestampMessage;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
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
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

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
        assertTrue(waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<String> res = DistributionZonesUtil.dataNodes(ByteUtils.fromBytes(dataNodes)).stream()
                            .map(NodeWithAttributes::nodeName)
                            .collect(Collectors.toSet());

                    return res.equals(clusterNodes);
                },
                2000
        ));
    }

    public static void assertDataNodesForZoneWithAttributes(
            int zoneId,
            @Nullable Set<NodeWithAttributes> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertTrue(waitForCondition(
                () -> {
                    byte[] dataNodes = keyValueStorage.get(zoneDataNodesKey(zoneId).bytes()).value();

                    if (dataNodes == null) {
                        return clusterNodes == null;
                    }

                    Set<NodeWithAttributes> res = DistributionZonesUtil.dataNodes(ByteUtils.fromBytes(dataNodes));

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
        assertTrue(
                waitForCondition(
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
        assertTrue(
                waitForCondition(
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
        assertTrue(
                waitForCondition(
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
            @Nullable Set<LogicalNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        byte[] nodes = clusterNodes == null
                ? null
                : toBytes(clusterNodes.stream().map(n -> new NodeWithAttributes(n.name(), n.nodeAttributes())).collect(Collectors.toSet()));

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zonesLogicalTopologyKey().bytes()).value(), nodes), 1000));
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyKey()} value.
     *
     * @param clusterNodes Expected cluster nodes' names.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopologyWithNodeNames(@Nullable Set<String> clusterNodes, KeyValueStorage keyValueStorage)
            throws InterruptedException {
        byte[] nodes = clusterNodes == null ? null : toBytes(clusterNodes);

        assertTrue(waitForCondition(() -> Arrays.equals(keyValueStorage.get(zonesLogicalTopologyKey().bytes()).value(), nodes), 1000));
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
        HybridTimestampMessage mockTsMessage = mock(HybridTimestampMessage.class);
        when(mockTsMessage.asHybridTimestamp()).thenReturn(new HybridTimestamp(10, 10));

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    if (cmd instanceof MetaStorageWriteCommand) {
                        ((MetaStorageWriteCommand) cmd).safeTime(mockTsMessage);
                    }

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
        ).when(metaStorageService).run(any(WriteCommand.class));

        lenient().doAnswer(
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
        ).when(metaStorageService).run(any(ReadCommand.class));

        MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

        lenient().doAnswer(invocationClose -> {
            Iif iif = invocationClose.getArgument(0);

            MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(iif).build();

            return metaStorageService.run(multiInvokeCommand);
        }).when(metaStorageManager).invoke(any());

        lenient().doAnswer(invocationClose -> {
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
        }).when(metaStorageManager).getAll(any());

        lenient().doAnswer(invocationClose -> {
            ByteArray key = invocationClose.getArgument(0);

            GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

            return metaStorageService.<Entry>run(getCommand).thenApply(
                    entry -> new EntryImpl(entry.key(), entry.value(), entry.revision(), entry.updateCounter())
            );
        }).when(metaStorageManager).get(any());
    }

    public static void mockVaultZonesLogicalTopologyKey(Set<LogicalNode> nodes, VaultManager vaultMgr) {
        Set<NodeWithAttributes> nodesWithAttributes = nodes.stream()
                .map(n -> new NodeWithAttributes(n.name(), n.nodeAttributes()))
                .collect(Collectors.toSet());

        byte[] newLogicalTopology = toBytes(nodesWithAttributes);

        when(vaultMgr.get(zonesLogicalTopologyKey()))
                .thenReturn(completedFuture(new VaultEntry(zonesLogicalTopologyKey(), newLogicalTopology)));
    }

    public static void watchListenerOnUpdate(Set<String> nodes, long rev, WatchListener topologyWatchListener) {
        byte[] newTopology = toBytes(nodes.stream()
                .map(n -> new NodeWithAttributes(n, Collections.emptyMap()))
                .collect(Collectors.toSet()));
        byte[] newTopVer = longToBytes(1L);

        Entry topology = new EntryImpl(zonesLogicalTopologyKey().bytes(), newTopology, rev, 1);
        Entry topVer = new EntryImpl(zonesLogicalTopologyVersionKey().bytes(), newTopVer, rev, 1);

        EntryEvent topologyEvent = new EntryEvent(null, topology);
        EntryEvent topVerEvent = new EntryEvent(null, topVer);

        WatchEvent evt = new WatchEvent(List.of(topologyEvent, topVerEvent), rev);

        topologyWatchListener.onUpdate(evt);
    }
}
