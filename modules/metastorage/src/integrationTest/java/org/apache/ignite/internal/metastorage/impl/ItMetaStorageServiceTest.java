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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.dsl.CompoundCondition.and;
import static org.apache.ignite.internal.metastorage.dsl.CompoundCondition.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.If.iif;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.impl.ItMetaStorageServiceTest.ServerConditionMatcher.cond;
import static org.apache.ignite.internal.metastorage.impl.ItMetaStorageServiceTest.ServerUpdateMatcher.upd;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.utils.ClusterServiceTestUtils.waitForTopology;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.server.AbstractCompoundCondition;
import org.apache.ignite.internal.metastorage.server.AbstractSimpleCondition;
import org.apache.ignite.internal.metastorage.server.AndCondition;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition.Type;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
public class ItMetaStorageServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItMetaStorageServiceTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 2;

    /**
     * Expected server result entry.
     */
    private static final EntryImpl EXPECTED_RESULT_ENTRY =
            new EntryImpl(
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

    /** Node 0 id. */
    private static final String NODE_ID_0 = "node-id-0";

    /** Node 1 id. */
    private static final String NODE_ID_1 = "node-id-1";

    /** Cluster. */
    private final ArrayList<ClusterService> cluster = new ArrayList<>();

    /** Meta storage raft server. */
    private final List<RaftManager> raftManagers = new ArrayList<>();

    /** List of Raft services (for resource management purposes). */
    private final List<RaftGroupService> raftGroupServices = new ArrayList<>();

    /** Mock Metastorage storage. */
    @Mock
    private KeyValueStorage mockStorage;

    /** Metastorage service. */
    private MetaStorageService metaStorageSvc;

    @WorkDirectory
    private Path dataPath;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    static {
        EXPECTED_RESULT_MAP = new TreeMap<>();

        EntryImpl entry1 = new EntryImpl(
                new byte[]{1},
                new byte[]{2},
                10,
                2
        );

        EXPECTED_RESULT_MAP.put(new ByteArray(entry1.key()), entry1);

        EntryImpl entry2 = new EntryImpl(
                new byte[]{3},
                new byte[]{4},
                10,
                3
        );

        EXPECTED_RESULT_MAP.put(new ByteArray(entry2.key()), entry2);

        EXPECTED_SRV_RESULT_COLL = List.of(entry1, entry2);
    }

    /**
     * Run {@code NODES} cluster nodes.
     */
    @BeforeEach
    public void beforeTest(TestInfo testInfo) throws Exception {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + NODES);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
                .map(addr -> ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder))
                .forEach(clusterService -> {
                    clusterService.start();
                    cluster.add(clusterService);
                });

        for (ClusterService node : cluster) {
            assertTrue(waitForTopology(node, NODES, 1000));
        }

        LOG.info("Cluster started.");

        metaStorageSvc = prepareMetaStorage();
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        Stream<AutoCloseable> stopRaftGroupServices = raftGroupServices.stream().map(service -> service::shutdown);

        Stream<AutoCloseable> stopRaftGroups = raftManagers.stream()
                .map(manager -> () -> manager.stopRaftNodes(MetastorageGroupId.INSTANCE));

        Stream<AutoCloseable> beforeNodeStop = Stream.concat(raftManagers.stream(), cluster.stream()).map(c -> c::beforeNodeStop);

        Stream<AutoCloseable> nodeStop = Stream.concat(raftManagers.stream(), cluster.stream()).map(c -> c::stop);

        IgniteUtils.closeAll(Stream.of(stopRaftGroupServices, stopRaftGroups, beforeNodeStop, nodeStop).flatMap(Function.identity()));
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key())).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(EXPECTED_RESULT_ENTRY, metaStorageSvc.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetWithUpperBoundRevision() throws Exception {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key(), EXPECTED_RESULT_ENTRY.revision())).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(
                EXPECTED_RESULT_ENTRY,
                metaStorageSvc.get(new ByteArray(EXPECTED_RESULT_ENTRY.key()), EXPECTED_RESULT_ENTRY.revision()).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        when(mockStorage.getAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(EXPECTED_RESULT_MAP, metaStorageSvc.getAll(EXPECTED_RESULT_MAP.keySet()).get());
    }

    /**
     * Tests {@link MetaStorageService#getAll(Set, long)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllWithUpperBoundRevision() throws Exception {
        when(mockStorage.getAll(anyList(), eq(10L))).thenReturn(EXPECTED_SRV_RESULT_COLL);

        assertEquals(
                EXPECTED_RESULT_MAP,
                metaStorageSvc.getAll(EXPECTED_RESULT_MAP.keySet(), 10).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#put(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = {2};

        doNothing().when(mockStorage).put(expKey.bytes(), expVal);

        metaStorageSvc.put(expKey, expVal).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndPut(ByteArray, byte[])}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPut() throws Exception {
        byte[] expVal = {2};

        when(mockStorage.getAndPut(EXPECTED_RESULT_ENTRY.key(), expVal)).thenReturn(EXPECTED_RESULT_ENTRY);

        assertEquals(
                EXPECTED_RESULT_ENTRY,
                metaStorageSvc.getAndPut(new ByteArray(EXPECTED_RESULT_ENTRY.key()), expVal).get()
        );
    }

    /**
     * Tests {@link MetaStorageService#putAll(Map)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        metaStorageSvc.putAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).putAll(keysCaptor.capture(), valuesCaptor.capture());

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
        when(mockStorage.getAndPutAll(anyList(), anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndPutAll(
                EXPECTED_RESULT_MAP.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().value())
                        )
        ).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<byte[]>> valuesCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).getAndPutAll(keysCaptor.capture(), valuesCaptor.capture());

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
        ByteArray expKey = new ByteArray(new byte[]{1});

        doNothing().when(mockStorage).remove(expKey.bytes());

        metaStorageSvc.remove(expKey).get();
    }

    /**
     * Tests {@link MetaStorageService#getAndRemove(ByteArray)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemove() throws Exception {
        EntryImpl expRes = new EntryImpl(
                new byte[]{1},
                new byte[]{3},
                10,
                2
        );

        when(mockStorage.getAndRemove(expRes.key())).thenReturn(expRes);

        assertEquals(expRes, metaStorageSvc.getAndRemove(new ByteArray(expRes.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#removeAll(Set)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        doNothing().when(mockStorage).removeAll(anyList());

        metaStorageSvc.removeAll(EXPECTED_RESULT_MAP.keySet()).get();

        List<byte[]> expKeys = EXPECTED_RESULT_MAP.keySet().stream()
                .map(ByteArray::bytes).collect(toList());

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).removeAll(keysCaptor.capture());

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
        when(mockStorage.getAndRemoveAll(anyList())).thenReturn(EXPECTED_SRV_RESULT_COLL);

        Map<ByteArray, Entry> gotRes = metaStorageSvc.getAndRemoveAll(EXPECTED_RESULT_MAP.keySet()).get();

        assertEquals(EXPECTED_RESULT_MAP, gotRes);

        ArgumentCaptor<List<byte[]>> keysCaptor = ArgumentCaptor.forClass(List.class);

        verify(mockStorage).getAndRemoveAll(keysCaptor.capture());

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
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        long expRevUpperBound = 10;

        when(mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes(), expRevUpperBound, false)).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, expKeyTo, expRevUpperBound).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with not null keyTo.
     *
     */
    @Test
    public void testRangeWitKeyTo() {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        ByteArray expKeyTo = new ByteArray(new byte[]{3});

        when(mockStorage.range(expKeyFrom.bytes(), expKeyTo.bytes(), false)).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, expKeyTo).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} with null keyTo.
     *
     */
    @Test
    public void testRangeWitNullAsKeyTo() {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        when(mockStorage.range(expKeyFrom.bytes(), null, false)).thenReturn(mock(Cursor.class));

        metaStorageSvc.range(expKeyFrom, null).close();
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} hasNext.
     */
    @Test
    public void testRangeHasNext() {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        when(mockStorage.range(expKeyFrom.bytes(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);

            return cursor;
        });

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        assertTrue(cursor.iterator().hasNext());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next.
     */
    @Test
    public void testRangeNext() {
        when(mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_RESULT_ENTRY);

            return cursor;
        });

        Cursor<Entry> cursor = metaStorageSvc.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertEquals(EXPECTED_RESULT_ENTRY, cursor.iterator().next());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}'s cursor exceptional case.
     */
    @Test
    public void testRangeNextNoSuchElementException() {
        when(mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenThrow(new NoSuchElementException());

            return cursor;
        });

        Cursor<Entry> cursor = metaStorageSvc.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertThrows(NoSuchElementException.class, () -> cursor.iterator().next());
    }

    /**
     * Tests {@link MetaStorageService#range(ByteArray, ByteArray, long)}} close.
     *
     */
    @Test
    public void testRangeClose() {
        ByteArray expKeyFrom = new ByteArray(new byte[]{1});

        Cursor cursorMock = mock(Cursor.class);

        when(mockStorage.range(expKeyFrom.bytes(), null, false)).thenReturn(cursorMock);

        Cursor<Entry> cursor = metaStorageSvc.range(expKeyFrom, null);

        cursor.close();

        verify(cursorMock, times(1)).close();
    }

    @Test
    public void testWatchOnUpdate() throws Exception {
        WatchEvent expectedEvent =
                new WatchEvent(List.of(
                        new EntryEvent(
                                new EntryImpl(
                                        new byte[]{2},
                                        new byte[]{20},
                                        1,
                                        1
                                ),
                                new EntryImpl(
                                        new byte[]{2},
                                        new byte[]{21},
                                        2,
                                        4
                                )
                        ),
                        new EntryEvent(
                                new EntryImpl(
                                        new byte[]{3},
                                        new byte[]{20},
                                        1,
                                        2
                                ),
                                new EntryImpl(
                                        new byte[]{3},
                                        new byte[]{},
                                        2,
                                        5
                                )
                        ),
                        new EntryEvent(
                                new EntryImpl(
                                        new byte[]{4},
                                        new byte[]{20},
                                        1,
                                        3
                                ),
                                new EntryImpl(
                                        new byte[]{4},
                                        new byte[]{},
                                        3,
                                        6
                                )
                        )
                ));

        ByteArray keyFrom = new ByteArray(new byte[]{1});

        ByteArray keyTo = new ByteArray(new byte[]{10});

        long rev = 2;

        when(mockStorage.watch(keyFrom.bytes(), keyTo.bytes(), rev)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(expectedEvent);

            return cursor;
        });

        CountDownLatch latch = new CountDownLatch(1);

        IgniteUuid watchId = metaStorageSvc.watch(keyFrom, keyTo, rev, new WatchListener() {
            @Override
            public boolean onUpdate(@NotNull WatchEvent event) {
                Collection<EntryEvent> expectedEvents = expectedEvent.entryEvents();
                Collection<EntryEvent> actualEvents = event.entryEvents();

                assertEquals(expectedEvents.size(), actualEvents.size());

                Iterator<EntryEvent> expectedIterator = expectedEvents.iterator();
                Iterator<EntryEvent> actualIterator = actualEvents.iterator();

                while (expectedIterator.hasNext() && actualIterator.hasNext()) {
                    EntryEvent expectedEntryEvent = expectedIterator.next();
                    EntryEvent actualEntryEvent = actualIterator.next();

                    assertArrayEquals(expectedEntryEvent.oldEntry().key(), actualEntryEvent.oldEntry().key());
                    assertArrayEquals(expectedEntryEvent.oldEntry().value(), actualEntryEvent.oldEntry().value());
                    assertArrayEquals(expectedEntryEvent.newEntry().key(), actualEntryEvent.newEntry().key());
                    assertArrayEquals(expectedEntryEvent.newEntry().value(), actualEntryEvent.newEntry().value());
                }

                latch.countDown();

                return true;
            }

            @Override
            public void onError(@NotNull Throwable e) {
                // Within given test it's not expected to get here.
                fail();
            }
        }).get();

        latch.await();

        metaStorageSvc.stopWatch(watchId).get();
    }

    @Test
    public void testMultiInvoke() throws Exception {
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

        var iif = If.iif(or(value(key1).eq(val1), value(key2).ne(val2)),
                iif(or(revision(key3).eq(3), or(value(key2).gt(val1), value(key1).ge(val2))),
                        ops(put(key1, rval1)).yield(true),
                        iif(and(value(key2).lt(val1), value(key1).le(val2)),
                                ops(put(key1, rval1), remove(key2)).yield(false),
                                ops().yield(true))),
                ops(put(key2, rval2)).yield(false));

        var ifCaptor = ArgumentCaptor.forClass(org.apache.ignite.internal.metastorage.server.If.class);

        when(mockStorage.invoke(any())).thenReturn(new StatementResult(true));

        assertTrue(metaStorageSvc.invoke(iif).get().getAsBoolean());

        verify(mockStorage).invoke(ifCaptor.capture());

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

        assertThat(resultIf.andThen().iif().andThen().update(), upd(new Update(
                List.of(Operation.put(key1.bytes(), rval1)),
                new StatementResult(true))));

        assertThat(resultIf.andThen().iif().orElse().iif().andThen().update(), upd(new Update(
                Arrays.asList(Operation.put(key1.bytes(), rval1), Operation.remove(key2.bytes())),
                new StatementResult(false))));

        assertThat(resultIf.andThen().iif().orElse().iif().orElse().update(),
                upd(new Update(Collections.emptyList(), new StatementResult(true))));

        assertThat(resultIf.orElse().update(), upd(new Update(
                List.of(Operation.put(key2.bytes(), rval2)),
                new StatementResult(false))));
    }

    @Test
    public void testInvoke() throws Exception {
        ByteArray expKey = new ByteArray(new byte[]{1});

        byte[] expVal = {2};

        when(mockStorage.invoke(any(), any(), any())).thenReturn(true);

        Condition condition = Conditions.notExists(expKey);

        Operation success = Operations.put(expKey, expVal);

        Operation failure = Operations.noop();

        assertTrue(metaStorageSvc.invoke(condition, success, failure).get());

        var conditionCaptor = ArgumentCaptor.forClass(AbstractSimpleCondition.class);

        ArgumentCaptor<Collection<Operation>> successCaptor = ArgumentCaptor.forClass(Collection.class);

        ArgumentCaptor<Collection<Operation>> failureCaptor = ArgumentCaptor.forClass(Collection.class);

        verify(mockStorage).invoke(conditionCaptor.capture(), successCaptor.capture(), failureCaptor.capture());

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
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key()))
                .thenThrow(new org.apache.ignite.internal.metastorage.server.CompactedException());

        assertThrows(CompactedException.class, () -> metaStorageSvc.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#get(ByteArray)}.
     */
    @Disabled // TODO: IGNITE-14693 Add tests for exception handling logic.
    @Test
    public void testGetThatThrowsOperationTimeoutException() {
        when(mockStorage.get(EXPECTED_RESULT_ENTRY.key())).thenThrow(new OperationTimeoutException());

        assertThrows(OperationTimeoutException.class, () -> metaStorageSvc.get(new ByteArray(EXPECTED_RESULT_ENTRY.key())).get());
    }

    /**
     * Tests {@link MetaStorageService#closeCursors(String)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCursorsCleanup() throws Exception {
        when(mockStorage.range(EXPECTED_RESULT_ENTRY.key(), null, false)).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(true);
            when(cursor.next()).thenReturn(EXPECTED_RESULT_ENTRY);

            return cursor;
        });

        String localName = cluster.get(0).topologyService().localMember().name();

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(localName));

        RaftGroupService metaStorageRaftSvc2 = raftManagers.get(1)
                .startRaftGroupService(MetastorageGroupId.INSTANCE, configuration)
                .get(3, TimeUnit.SECONDS);

        raftGroupServices.add(metaStorageRaftSvc2);

        MetaStorageService metaStorageSvc2 = new MetaStorageServiceImpl(metaStorageRaftSvc2, NODE_ID_1, NODE_ID_1);

        Cursor<Entry> cursorNode0 = metaStorageSvc.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursorNode0.hasNext());

        Cursor<Entry> cursor2Node0 = metaStorageSvc.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursor2Node0.hasNext());

        Cursor<Entry> cursorNode1 = metaStorageSvc2.range(new ByteArray(EXPECTED_RESULT_ENTRY.key()), null);

        assertTrue(cursorNode1.hasNext());

        metaStorageSvc.closeCursors(NODE_ID_0).get();

        assertThrows(NoSuchElementException.class, () -> cursorNode0.iterator().next());

        assertThrows(NoSuchElementException.class, () -> cursor2Node0.iterator().next());

        assertEquals(EXPECTED_RESULT_ENTRY, cursorNode1.iterator().next());
    }

    /**
     * Matcher for {@link Update}.
     */
    protected static class ServerUpdateMatcher extends TypeSafeMatcher<Update> {

        private final Update update;

        public ServerUpdateMatcher(Update update) {
            this.update = update;
        }

        public static ServerUpdateMatcher upd(Update update) {
            return new ServerUpdateMatcher(update);
        }

        @Override
        protected boolean matchesSafely(Update item) {
            return item.operations().size() == update.operations().size()
                    && Arrays.equals(item.result().bytes(), update.result().bytes())
                    && opsEqual(item.operations().iterator(), update.operations().iterator());

        }

        @Override
        public void describeTo(Description description) {
            description.appendText(toString(update));
        }

        @Override
        protected void describeMismatchSafely(Update item, Description mismatchDescription) {
            mismatchDescription.appendText(toString(item));
        }

        private String toString(Update upd) {
            return "Update([" + upd.operations().stream()
                    .map(o -> o.type() + "(" + Arrays.toString(o.key()) + ", " + Arrays.toString(o.value()) + ")")
                    .collect(Collectors.joining(",")) + "])";
        }

        private boolean opsEqual(Iterator<Operation> ops1, Iterator<Operation> ops2) {
            if (!ops1.hasNext()) {
                return true;
            } else {
                return opEqual(ops1.next(), ops2.next()) && opsEqual(ops1, ops2);
            }
        }

        private boolean opEqual(Operation op1, Operation op2) {
            return Arrays.equals(op1.key(), op2.key()) && Arrays.equals(op1.value(), op2.value()) && op1.type() == op2.type();
        }
    }

    /**
     * Matcher for {@link org.apache.ignite.internal.metastorage.server.Condition}.
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

    /**
     * Prepares meta storage by instantiating corresponding raft server with {@link MetaStorageListener} and {@link
     * MetaStorageServiceImpl}.
     *
     * @return {@link MetaStorageService} instance.
     */
    private MetaStorageService prepareMetaStorage() throws Exception {
        String localName = cluster.get(0).topologyService().localMember().name();

        PeersAndLearners configuration = PeersAndLearners.fromConsistentIds(Set.of(localName));

        CompletableFuture<RaftGroupService> firstService = startRaftService(cluster.get(0), configuration);
        CompletableFuture<RaftGroupService> secondService = startRaftService(cluster.get(1), configuration);

        assertThat(allOf(firstService, secondService), willCompleteSuccessfully());

        raftGroupServices.add(firstService.join());
        raftGroupServices.add(secondService.join());

        RaftGroupService metaStorageRaftGrpSvc = secondService.join();

        return new MetaStorageServiceImpl(metaStorageRaftGrpSvc, NODE_ID_0, NODE_ID_0);
    }

    private CompletableFuture<RaftGroupService> startRaftService(
            ClusterService node, PeersAndLearners configuration
    ) throws NodeStoppingException {
        var raftManager = new Loza(
                node,
                raftConfiguration,
                dataPath.resolve("raftManager" + raftManagers.size()),
                new HybridClockImpl());

        raftManager.start();

        raftManagers.add(raftManager);

        Peer serverPeer = configuration.peer(node.topologyService().localMember().name());

        if (serverPeer == null) {
            return raftManager.startRaftGroupService(MetastorageGroupId.INSTANCE, configuration);
        } else {
            var nodeId = new RaftNodeId(MetastorageGroupId.INSTANCE, serverPeer);

            return raftManager.startRaftGroupNode(
                    nodeId, configuration, new MetaStorageListener(mockStorage), RaftGroupEventsListener.noopLsnr
            );
        }
    }
}
