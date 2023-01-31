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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.metastorage.impl.ItMetaStorageServiceTest.ServerConditionMatcher.cond;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.server.AbstractCompoundCondition;
import org.apache.ignite.internal.metastorage.server.AbstractSimpleCondition;
import org.apache.ignite.internal.metastorage.server.AndCondition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition.Type;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageLearnerListener;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * Meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItMetaStorageServiceTest {
    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Expected result entry. */
    private static final Entry EXPECTED_RESULT_ENTRY = new EntryImpl(
            new byte[]{1},
            new byte[]{2},
            10,
            2
    );

    /**
     * Expected result map.
     */
    private static final NavigableMap<ByteArray, Entry> EXPECTED_RESULT_MAP;

    /** Expected server result collection. */
    private static final Collection<Entry> EXPECTED_SRV_RESULT_COLL;

    static {
        EXPECTED_RESULT_MAP = new TreeMap<>();

        Entry entry1 = new EntryImpl(
                new byte[]{1},
                new byte[]{2},
                10,
                2
        );

        EXPECTED_RESULT_MAP.put(new ByteArray(entry1.key()), entry1);

        Entry entry2 = new EntryImpl(
                new byte[]{3},
                new byte[]{4},
                10,
                3
        );

        EXPECTED_RESULT_MAP.put(new ByteArray(entry2.key()), entry2);

        EXPECTED_SRV_RESULT_COLL = List.of(entry1, entry2);
    }

    private static class Node {
        private final ClusterService clusterService;

        private final RaftManager raftManager;

        private final KeyValueStorage mockStorage;

        private RaftGroupService metaStorageRaftService;

        private MetaStorageService metaStorageService;

        Node(ClusterService clusterService, RaftConfiguration raftConfiguration, Path dataPath) {
            this.clusterService = clusterService;

            this.raftManager = new Loza(
                    clusterService,
                    raftConfiguration,
                    dataPath.resolve(name()),
                    new HybridClockImpl()
            );

            this.mockStorage = mock(KeyValueStorage.class);
        }

        void start(PeersAndLearners configuration) {
            clusterService.start();
            raftManager.start();

            CompletableFuture<RaftGroupService> raftService = startRaftService(configuration);

            assertThat(raftService, willCompleteSuccessfully());

            metaStorageRaftService = raftService.join();

            ClusterNode node = clusterService.topologyService().localMember();

            metaStorageService = new MetaStorageServiceImpl(metaStorageRaftService, node);
        }

        String name() {
            return clusterService.localConfiguration().getName();
        }

        private CompletableFuture<RaftGroupService> startRaftService(PeersAndLearners configuration) {
            String name = name();

            boolean isLearner = configuration.peer(name) == null;

            Peer peer = isLearner ? configuration.learner(name) : configuration.peer(name);

            assert peer != null;

            RaftGroupListener listener = isLearner ? new MetaStorageLearnerListener(mockStorage) : new MetaStorageListener(mockStorage);

            var raftNodeId = new RaftNodeId(MetastorageGroupId.INSTANCE, peer);

            try {
                return raftManager.startRaftGroupNode(raftNodeId, configuration, listener, RaftGroupEventsListener.noopLsnr);
            } catch (NodeStoppingException e) {
                throw new IllegalStateException(e);
            }
        }

        void stop() throws Exception {
            Stream<AutoCloseable> raftStop = Stream.of(
                    metaStorageRaftService == null ? null : (AutoCloseable) metaStorageRaftService::shutdown,
                    () -> raftManager.stopRaftNodes(MetastorageGroupId.INSTANCE)
            );

            Stream<AutoCloseable> beforeNodeStop = Stream.of(raftManager, clusterService).map(c -> c::beforeNodeStop);

            Stream<AutoCloseable> nodeStop = Stream.of(raftManager, clusterService).map(c -> c::stop);

            IgniteUtils.closeAll(Stream.of(raftStop, beforeNodeStop, nodeStop).flatMap(Function.identity()));
        }
    }

    private TestInfo testInfo;

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    private final List<Node> nodes = new ArrayList<>();

    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    private List<Node> startNodes(int amount) {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + amount);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
                .map(addr -> ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder))
                .forEach(clusterService -> nodes.add(new Node(clusterService, raftConfiguration, workDir)));

        PeersAndLearners metaStorageConfiguration = PeersAndLearners.fromConsistentIds(
                Set.of(nodes.get(0).name()),
                nodes.stream().skip(1).map(Node::name).collect(toUnmodifiableSet())
        );

        nodes.parallelStream().forEach(node -> node.start(metaStorageConfiguration));

        return nodes;
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.get(EXPECTED_RESULT_ENTRY.key())).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(EXPECTED_RESULT_ENTRY, node.metaStorageService.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetWithUpperBoundRevision() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.get(EXPECTED_RESULT_ENTRY.key(), EXPECTED_RESULT_ENTRY.revision())).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(
                EXPECTED_RESULT_ENTRY,
                node.metaStorageService.get(new ByteArray(EXPECTED_RESULT_ENTRY.key()), EXPECTED_RESULT_ENTRY.revision()).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.getAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(EXPECTED_RESULT_MAP, node.metaStorageService.getAll(EXPECTED_RESULT_MAP.keySet()).get());
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllWithUpperBoundRevision() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.getAll(anyList(), eq(10L))).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(
                EXPECTED_RESULT_MAP,
                node.metaStorageService.getAll(EXPECTED_RESULT_MAP.keySet(), 10).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#put(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        Node node = startNodes(1).get(0);

        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = {2};

        doNothing().when(node.mockStorage).put(expKey.bytes(), expVal);

        node.metaStorageService.put(expKey, expVal).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndPut(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPut() throws Exception {
        Node node = startNodes(1).get(0);

        byte[] expVal = {2};

        when(node.mockStorage.getAndPut(EXPECTED_RESULT_ENTRY.key(), expVal)).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(
                EXPECTED_RESULT_ENTRY,
                node.metaStorageService.getAndPut(new ByteArray(EXPECTED_RESULT_ENTRY.key()), expVal).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#putAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        Node node = startNodes(1).get(0);

        node.metaStorageService.putAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(node.mockStorage).putAll(keysCaptor.capture(), valuesCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream()
                .map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
        }

        // Assert values equality.
        assertEquals(EXPECTED_RESULT_MAP.values().size(), valuesCaptor.getValue().size());

        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream()
                .map(Entry::value).collect(toList());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expVals.get(i), valuesCaptor.getValue().get(i));
        }
    }

    /**
     * Tests {@link MetaStorageService#getAndPutAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutAll() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.getAndPutAll(anyList(), anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = node.metaStorageService.getAndPutAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(node.mockStorage).getAndPutAll(keysCaptor.capture(), valuesCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream()
                .map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
        }

        // Assert values equality.
        assertEquals(EXPECTED_RESULT_MAP.values().size(), valuesCaptor.getValue().size());

        List<byte[]> expVals = EXPECTED_RESULT_MAP.values().stream()
                .map(Entry::value).collect(toList());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expVals.get(i), valuesCaptor.getValue().get(i));
        }
    }

    /**
     * Tests {@link MetaStorageService#remove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        Node node = startNodes(1).get(0);

        ByteArray expKey = new ByteArray(new byte[]{1});

        doNothing().when(node.mockStorage).remove(expKey.bytes());

        node.metaStorageService.remove(expKey).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndRemove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemove() throws Exception {
        Node node = startNodes(1).get(0);

        Entry expRes = new EntryImpl(
                new byte[]{1},
                new byte[]{3},
                10,
                2
        );

        when(node.mockStorage.getAndRemove(expRes.key())).thenReturn(expRes);

        assertEquals(expRes, node.metaStorageService.getAndRemove(new ByteArray(expRes.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#removeAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        Node node = startNodes(1).get(0);

        node.metaStorageService.removeAll(EXPECTED_RESULT_MAP.keySet()).get();

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream()
                .map(ByteArray::bytes).collect(toList());

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(node.mockStorage).removeAll(keysCaptor.capture());

        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
        }
    }

    /**
     * Tests {@link MetaStorageService#getAndRemoveAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemoveAll() throws Exception {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.getAndRemoveAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = node.metaStorageService.getAndRemoveAll(EXPECTED_RESULT_MAP.keySet()).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(node.mockStorage).getAndRemoveAll(keysCaptor.capture());

        // Assert keys equality.
        assertEquals(EXPECTED_RESULT_MAP.keySet().size(), keysCaptor.getValue().size());

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream()
                .map(ByteArray::bytes).collect(toList());

        for (int i = 0; i < expKeys.size(); i++) {
            assertArrayEquals(expKeys.get(i), keysCaptor.getValue().get(i));
        }
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo and explicit revUpperBound.
     *
     */
    @Test
    public void testRangeWitKeyToAndUpperBound() {
        Node node = startNodes(1).get(0);

        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        long expRevUpperBound = 10;

        when(node.mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes(), expRevUpperBound, false)).thenReturn(mock(Cursor.class));

        node.metaStorageService.range(expKeyFrom, expKeyTo, expRevUpperBound).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo.
     *
     */
    @Test
    public void testRangeWitKeyTo() {
        Node node = startNodes(1).get(0);

        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        when(node.mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes(), false)).thenReturn(mock(Cursor.class));

        node.metaStorageService.range(expKeyFrom, expKeyTo).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with null keyTo.
     *
     */
    @Test
    public void testRangeWitNullAsKeyTo() {
        Node node = startNodes(1).get(0);

        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        when(node.mockStorage.range(expKeyFrom.bytes(), null, false)).thenReturn(mock(Cursor.class));

        node.metaStorageService.range(expKeyFrom, null).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} hasNext.
     */
    @Test
    public void testRangeHasNext() {
        Node node = startNodes(1).get(0);

        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        when(node.mockStorage.range(expKeyFrom.bytes(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);

            return cursor;
        });

        Cursor<Entry> cursor = node.metaStorageService.range(expKeyFrom, null);

        assertTrue(cursor.iterator().hasNext());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next.
     */
    @Test
    public void testRangeNext() {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_RESULT_ENTRY);

            return cursor;
        });

        Cursor<Entry> cursor = node.metaStorageService.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertEquals(EXPECTED_RESULT_ENTRY, cursor.iterator().next());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}'s cursor exceptional case.
     */
    @Test
    public void testRangeNextNoSuchElementException() {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenThrow(new NoSuchElementException());

            return cursor;
        });

        Cursor<Entry> cursor = node.metaStorageService.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertThrows(NoSuchElementException.class, () -> cursor.iterator().next());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} close.
     *
     */
    @Test
    public void testRangeClose() {
        Node node = startNodes(1).get(0);

        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        Cursor<Entry> cursorMock = mock(Cursor.class);

        when(node.mockStorage.range(expKeyFrom.bytes(), null, false)).thenReturn(cursorMock);

        Cursor<Entry> cursor = node.metaStorageService.range(expKeyFrom, null);

        cursor.close();

        verify(cursorMock, times(1)).close();
    }

    @Test
    public void testMultiInvoke() throws Exception {
        Node node = startNodes(1).get(0);

        ByteArray key1 = new ByteArray(new byte[]{1});
        ByteArray key2 = new ByteArray(new byte[]{2});
        ByteArray key3 = new ByteArray(new byte[]{3});

        var val1 = new byte[]{4};
        var val2 = new byte[]{5};

        var rval1 = new byte[]{6};
        var rval2 = new byte[]{7};

        /*
        if (key1.value == val1 || key2.value != val2)
            if (key3.revision == 3 || key2.value > val1 || key1.value >= val2):
                put(key1, rval1)
                return true
            else
                if (key2.value < val1 && key1.value <= val2):
                    put(key1, rval1)
                    remove(key2, rval2)
                    return false
                else
                    return true
        else
            put(key2, rval2)
            return false
         */

        var iif = iif(or(value(key1).eq(val1), value(key2).ne(val2)),
                iif(or(revision(key3).eq(3), or(value(key2).gt(val1), value(key1).ge(val2))),
                        ops(put(key1, rval1)).yield(true),
                        iif(and(value(key2).lt(val1), value(key1).le(val2)),
                                ops(put(key1, rval1), remove(key2)).yield(false),
                                ops().yield(true))),
                ops(put(key2, rval2)).yield(false));

        var ifCaptor = ArgumentCaptor.forClass(If.class);

        when(node.mockStorage.invoke(any())).thenReturn(ops().yield(true).result());

        assertTrue(node.metaStorageService.invoke(iif).get().getAsBoolean());

        verify(node.mockStorage).invoke(ifCaptor.capture());

        var resultIf = ifCaptor.getValue();

        assertThat(resultIf.cond(), cond(new OrCondition(new ValueCondition(Type.EQUAL, key1.bytes(), val1),
                new ValueCondition(Type.NOT_EQUAL, key2.bytes(), val2))));

        assertThat(resultIf.andThen().iif().cond(),
                cond(new OrCondition(new RevisionCondition(RevisionCondition.Type.EQUAL, key3.bytes(), 3),
                        new OrCondition(new ValueCondition(ValueCondition.Type.GREATER, key2.bytes(), val1), new ValueCondition(
                                Type.GREATER_OR_EQUAL, key1.bytes(), val2)))));

        assertThat(resultIf.andThen().iif().orElse().iif().cond(),
                cond(new AndCondition(new ValueCondition(ValueCondition.Type.LESS, key2.bytes(), val1), new ValueCondition(
                        Type.LESS_OR_EQUAL, key1.bytes(), val2))));

        assertThat(
                resultIf.andThen().iif().andThen().update(),
                is(ops(put(key1, rval1)).yield(true))
        );

        assertThat(
                resultIf.andThen().iif().orElse().iif().andThen().update(),
                is(ops(put(key1, rval1), remove(key2)).yield(false))
        );

        assertThat(
                resultIf.andThen().iif().orElse().iif().orElse().update(),
                is(ops().yield(true))
        );

        assertThat(
                resultIf.orElse().update(),
                is(ops(put(key2, rval2)).yield(false))
        );
    }

    @Test
    public void testInvoke() throws Exception {
        Node node = startNodes(1).get(0);

        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = {2};

        when(node.mockStorage.invoke(any(), any(), any())).thenReturn(true);

        Condition condition = Conditions.notExists(expKey);

        Operation success = Operations.put(expKey, expVal);

        Operation failure = Operations.noop();

        assertTrue(node.metaStorageService.invoke(condition, success, failure).get());

        var conditionCaptor = ArgumentCaptor.forClass(AbstractSimpleCondition.class);

        ArgumentCaptor<Collection<Operation>> successCaptor = ArgumentCaptor.forClass(Collection.class);

        ArgumentCaptor<Collection<Operation>> failureCaptor = ArgumentCaptor.forClass(Collection.class);

        verify(node.mockStorage).invoke(conditionCaptor.capture(), successCaptor.capture(), failureCaptor.capture());

        assertArrayEquals(expKey.bytes(), conditionCaptor.getValue().key());

        assertArrayEquals(expKey.bytes(), successCaptor.getValue().iterator().next().key());
        assertArrayEquals(expVal, successCaptor.getValue().iterator().next().value());

        assertEquals(OperationType.NO_OP, failureCaptor.getValue().iterator().next().type());
    }

    // TODO: IGNITE-14693 Add tests for exception handling logic: onError,
    // TODO: (CompactedException | OperationTimeoutException)

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsCompactedException() {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.get(EXPECTED_RESULT_ENTRY.key())).thenThrow(new CompactedException());

        assertThrows(CompactedException.class, () -> node.metaStorageService.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsOperationTimeoutException() {
        Node node = startNodes(1).get(0);

        when(node.mockStorage.get(EXPECTED_RESULT_ENTRY.key())).thenThrow(new OperationTimeoutException());

        assertThrows(OperationTimeoutException.class, () -> node.metaStorageService.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#closeCursors(String)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCursorsCleanup() throws Exception {
        startNodes(2);

        Node leader = nodes.get(0);
        Node learner = nodes.get(1);

        when(leader.mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_RESULT_ENTRY);

            return cursor;
        });

        Cursor<Entry> cursorNode0 = leader.metaStorageService.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursorNode0.hasNext());

        Cursor<Entry> cursor2Node0 = leader.metaStorageService.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursor2Node0.hasNext());

        Cursor<Entry> cursorNode1 = learner.metaStorageService.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursorNode1.hasNext());

        leader.metaStorageService.closeCursors(leader.clusterService.topologyService().localMember().id()).get();

        assertThrows(NoSuchElementException.class, () -> cursorNode0.iterator().next());

        assertThrows(NoSuchElementException.class, () -> cursor2Node0.iterator().next());

        assertEquals(EXPECTED_RESULT_ENTRY, cursorNode1.iterator().next());
    }

    /**
     * Matcher for {@link Condition}.
     */
    protected static class ServerConditionMatcher extends TypeSafeMatcher<org.apache.ignite.internal.metastorage.server.Condition> {

        private final org.apache.ignite.internal.metastorage.server.Condition condition;

        public ServerConditionMatcher(org.apache.ignite.internal.metastorage.server.Condition condition) {
            this.condition = condition;
        }

        public static ServerConditionMatcher cond(org.apache.ignite.internal.metastorage.server.Condition condition) {
            return new ServerConditionMatcher(condition);
        }

        @Override
        protected boolean matchesSafely(org.apache.ignite.internal.metastorage.server.Condition item) {
            if (condition.getClass() == item.getClass() && Arrays.deepEquals(condition.keys(), item.keys())) {
                if (condition.getClass().isInstance(AbstractCompoundCondition.class)) {
                    return new ServerConditionMatcher(((AbstractCompoundCondition) condition).leftCondition())
                            .matchesSafely(((AbstractCompoundCondition) item).leftCondition())
                            && new ServerConditionMatcher(((AbstractCompoundCondition) condition).rightCondition())
                                    .matchesSafely(((AbstractCompoundCondition) item).rightCondition());
                } else {
                    return true;
                }
            }

            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(toString(condition));
        }

        @Override
        protected void describeMismatchSafely(org.apache.ignite.internal.metastorage.server.Condition item,
                Description mismatchDescription) {
            mismatchDescription.appendText(toString(item));
        }

        private String toString(org.apache.ignite.internal.metastorage.server.Condition cond) {
            if (cond instanceof AbstractSimpleCondition) {
                return cond.getClass().getSimpleName() + "(" + Arrays.deepToString(cond.keys()) + ")";
            } else if (cond instanceof AbstractCompoundCondition) {
                return cond.getClass() + "(" + toString(((AbstractCompoundCondition) cond).leftCondition()) + ", " + toString(
                        ((AbstractCompoundCondition) cond).rightCondition()) + ")";
            } else {
                throw new IllegalArgumentException("Unknown condition type " + cond.getClass().getSimpleName());
            }

        }
    }
}
