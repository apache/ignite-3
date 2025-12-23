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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager.configureCmgManagerToStartMetastorage;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.disaster.system.repair.MetastorageRepair;
import org.apache.ignite.internal.disaster.system.storage.MetastorageRepairStorage;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.TestLozaFactory;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.ReadActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Integration tests for {@link MetaStorageManagerImpl}.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class ItMetaStorageManagerImplTest extends IgniteAbstractTest {
    private static final ByteArray FOO_KEY = new ByteArray("foo");

    private static final byte[] VALUE = "value".getBytes(UTF_8);

    private ClusterService clusterService;

    private Loza raftManager;

    private LogStorageFactory partitionsLogStorageFactory;

    private LogStorageFactory msLogStorageFactory;

    private KeyValueStorage storage;

    private MetaStorageManagerImpl metaStorageManager;

    private HybridClock clock;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    private final ReadOperationForCompactionTracker readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration("mock.idleSafeTimeSyncIntervalMillis = 100") SystemDistributedConfiguration systemConfiguration
    ) {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        clock = new HybridClockImpl();

        var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        ComponentWorkingDir workingDir = new ComponentWorkingDir(workDir.resolve("loza"));

        partitionsLogStorageFactory = SharedLogStorageFactoryUtils.create(
                clusterService.nodeName(),
                workingDir.raftLogPath()
        );

        raftManager = TestLozaFactory.create(
                clusterService,
                raftConfiguration,
                systemLocalConfiguration,
                clock,
                raftGroupEventsClientListener
        );

        var logicalTopologyService = mock(LogicalTopologyService.class);

        when(logicalTopologyService.validatedNodesOnLeader()).thenReturn(emptySetCompletedFuture());

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageInfo()).thenReturn(completedFuture(
                new CmgMessagesFactory().metaStorageInfo().metaStorageNodes(Set.of(clusterService.nodeName())).build()
        ));
        configureCmgManagerToStartMetastorage(cmgManager);

        ComponentWorkingDir metastorageWorkDir = new ComponentWorkingDir(workDir.resolve("metastorage"));

        msLogStorageFactory =
                SharedLogStorageFactoryUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

        RaftGroupOptionsConfigurer msRaftConfigurer =
                RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

        storage = new RocksDbKeyValueStorage(
                clusterService.nodeName(),
                metastorageWorkDir.dbPath(),
                new NoOpFailureManager(),
                readOperationForCompactionTracker,
                scheduledExecutorService
        );

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                storage,
                clock,
                topologyAwareRaftGroupServiceFactory,
                new NoOpMetricManager(),
                systemConfiguration,
                msRaftConfigurer,
                readOperationForCompactionTracker
        );

        assertThat(
                startAsync(new ComponentContext(),
                        clusterService, partitionsLogStorageFactory, msLogStorageFactory, raftManager, metaStorageManager)
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        List<IgniteComponent> components =
                List.of(metaStorageManager, raftManager, partitionsLogStorageFactory, msLogStorageFactory, clusterService);

        closeAll(Stream.concat(
                components.stream().map(c -> c::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully()))
        ));
    }

    /**
     * Tests a corner case when a prefix request contains a max unsigned byte value.
     */
    @Test
    void testPrefixOverflow() {
        var key1 = new ByteArray(new byte[]{1, (byte) 0xFF, 0});
        var key2 = new ByteArray(new byte[]{1, (byte) 0xFF, 1});
        var key3 = new ByteArray(new byte[]{1, (byte) 0xFF, (byte) 0xFF});
        // Contains the next lexicographical prefix
        var key4 = new ByteArray(new byte[]{1, 0, 1});
        // Contains the previous lexicographical prefix
        var key5 = new ByteArray(new byte[]{1, (byte) 0xFE, 1});

        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.notExists(FOO_KEY),
                List.of(
                        Operations.put(key1, VALUE),
                        Operations.put(key2, VALUE),
                        Operations.put(key3, VALUE),
                        Operations.put(key4, VALUE),
                        Operations.put(key5, VALUE)
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));

        CompletableFuture<List<byte[]>> actualKeysFuture =
                subscribeToList(metaStorageManager.prefix(new ByteArray(new byte[]{1, (byte) 0xFF})))
                        .thenApply(entries -> entries.stream().map(Entry::key).collect(toList()));

        assertThat(actualKeysFuture, will(contains(key1.bytes(), key2.bytes(), key3.bytes())));
    }

    @Test
    void testMetaStorageStopClosesRaftService() {
        MetaStorageServiceImpl svc = metaStorageManager.metaStorageService().join();

        assertThat(metaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<Entry> fut = svc.get(FOO_KEY);

        assertThat(fut, willThrowFast(NodeStoppingException.class));
    }

    @Test
    void testMetaStorageStopBeforeRaftServiceStarted() {
        // Close MetaStorage that is created in setUp.
        assertThat(metaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        Set<String> msNodes = Set.of(clusterService.nodeName());
        CompletableFuture<Set<String>> cmgFut = new CompletableFuture<>();

        when(cmgManager.metaStorageNodes()).thenReturn(cmgFut);

        metaStorageManager = new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                mock(LogicalTopologyService.class),
                raftManager,
                storage,
                clock,
                mock(TopologyAwareRaftGroupServiceFactory.class),
                new NoOpMetricManager(),
                mock(MetastorageRepairStorage.class),
                mock(MetastorageRepair.class),
                RaftGroupOptionsConfigurer.EMPTY,
                readOperationForCompactionTracker,
                ForkJoinPool.commonPool(),
                new NoOpFailureManager()
        );

        assertThat(metaStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        // Unblock the future so raft service can be initialized. Although the future should be cancelled already by the
        // stop method.
        cmgFut.complete(msNodes);

        assertThat(metaStorageManager.metaStorageService(), willThrow(NodeStoppingException.class));
    }

    @Test
    void testUpdateRevisionListener() {
        ArgumentCaptor<Long> revisionCapture = ArgumentCaptor.forClass(Long.class);

        RevisionUpdateListener listener = mock(RevisionUpdateListener.class);

        when(listener.onUpdated(revisionCapture.capture())).thenReturn(nullCompletedFuture());

        long revision = metaStorageManager.appliedRevision();

        metaStorageManager.registerRevisionUpdateListener(listener);

        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willSucceedFast());

        // Watches are processed asynchronously.
        // Timeout is big just in case there's a GC pause. Test's duration doesn't really depend on it.
        verify(listener, timeout(5000).atLeast(1)).onUpdated(anyLong());

        assertThat(revisionCapture.getAllValues(), is(List.of(revision + 1)));
    }

    /**
     * Tests that idle safe time propagation does not advance safe time while watches of a normal command are being executed.
     */
    @Test
    void testIdleSafeTimePropagationAndNormalSafeTimePropagationInteraction(TestInfo testInfo) {
        AtomicBoolean watchCompleted = new AtomicBoolean(false);
        CompletableFuture<HybridTimestamp> watchEventTsFuture = new CompletableFuture<>();

        metaStorageManager.registerExactWatch(FOO_KEY, event -> {
            watchEventTsFuture.complete(event.timestamp());

            // The future will set the flag and complete after 300ms to allow idle safe time mechanism (which ticks each 100ms)
            // to advance SafeTime (if there is still a bug for which this test is written).
            return waitFor(300, TimeUnit.MILLISECONDS)
                    .whenComplete((res, ex) -> watchCompleted.set(true));
        });

        metaStorageManager.put(FOO_KEY, VALUE);

        ClusterTime clusterTime = metaStorageManager.clusterTime();

        assertThat(watchEventTsFuture, willSucceedIn(5, TimeUnit.SECONDS));

        HybridTimestamp watchEventTs = watchEventTsFuture.join();
        assertThat(clusterTime.waitFor(watchEventTs), willCompleteSuccessfully());

        assertThat("Safe time is advanced too early", watchCompleted.get(), is(true));
    }

    @Test
    void testReadOperationsFutureWithoutReadOperations() {
        assertTrue(readOperationForCompactionTracker.collect(0).isDone());
        assertTrue(readOperationForCompactionTracker.collect(1).isDone());
    }

    /**
     * Tests tracking only read operations from the leader, local reads must be tracked by the {@link KeyValueStorage} itself.
     * <ul>
     *     <li>Creates read operations from the leader and local ones.</li>
     *     <li>Set a new compaction revision via {@link KeyValueStorage#setCompactionRevision}.</li>
     *     <li>Wait for the completion of read operations on the new compaction revision.</li>
     * </ul>
     *
     * <p>Due to the difficulty of testing all reading from leader methods at once, we test each of them separately.</p>
     */
    @ParameterizedTest
    @MethodSource("readFromLeaderOperations")
    public void testReadOperationsFuture(ReadFromLeaderAction readFromLeaderAction) {
        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());
        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());

        var startSendReadActionRequestFuture = new CompletableFuture<Void>();
        var continueSendReadActionRequestFuture = new CompletableFuture<Void>();

        listenReadActionRequest(startSendReadActionRequestFuture, continueSendReadActionRequestFuture);

        CompletableFuture<?> readFromLeaderOperationFuture = readFromLeaderAction.read(metaStorageManager, FOO_KEY);
        Cursor<Entry> getLocallyCursor = metaStorageManager.getLocally(FOO_KEY, FOO_KEY, 2);

        assertThat(startSendReadActionRequestFuture, willCompleteSuccessfully());

        storage.setCompactionRevision(1);

        CompletableFuture<Void> readOperationsFuture = readOperationForCompactionTracker.collect(2);
        assertFalse(readOperationsFuture.isDone());

        getLocallyCursor.close();
        assertFalse(readOperationsFuture.isDone());

        continueSendReadActionRequestFuture.complete(null);
        assertThat(readOperationsFuture, willCompleteSuccessfully());
        assertThat(readFromLeaderOperationFuture, willCompleteSuccessfully());
    }

    /**
     * Tests that read operations from the leader and local ones created after {@link KeyValueStorage#setCompactionRevision}
     * will not affect future from {@link ReadOperationForCompactionTracker#collect} on a new compaction revision.
     *
     * <p>Due to the difficulty of testing all reading from leader methods at once, we test each of them separately.</p>
     */
    @ParameterizedTest
    @MethodSource("readFromLeaderOperations")
    void testReadOperationsFutureForReadOperationAfterSetCompactionRevision(ReadFromLeaderAction readFromLeaderAction) throws Exception {
        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());
        assertThat(metaStorageManager.put(FOO_KEY, VALUE), willCompleteSuccessfully());

        storage.setCompactionRevision(1);

        var startSendReadActionRequestFuture = new CompletableFuture<Void>();
        var continueSendReadActionRequestFuture = new CompletableFuture<Void>();

        listenReadActionRequest(startSendReadActionRequestFuture, continueSendReadActionRequestFuture);

        CompletableFuture<?> readFromLeaderOperationFuture = readFromLeaderAction.read(metaStorageManager, FOO_KEY);
        Cursor<Entry> getLocallyCursor = metaStorageManager.getLocally(FOO_KEY, FOO_KEY, 2);

        assertThat(startSendReadActionRequestFuture, willCompleteSuccessfully());

        assertTrue(readOperationForCompactionTracker.collect(1).isDone());

        getLocallyCursor.close();

        continueSendReadActionRequestFuture.complete(null);
        assertThat(readFromLeaderOperationFuture, willCompleteSuccessfully());
    }

    @FunctionalInterface
    private interface ReadFromLeaderAction {
        CompletableFuture<?> read(MetaStorageManager metastore, ByteArray key);

        static ReadFromLeaderAction readAsync(ReadFromLeaderAction readFromLeaderAction) {
            return (metastore, key) -> runAsync(() -> readFromLeaderAction.read(metastore, key)).thenCompose(Function.identity());
        }
    }

    private static List<Arguments> readFromLeaderOperations() {
        return List.of(
                Arguments.of(Named.named(
                        "getSingleLatest",
                        ReadFromLeaderAction.readAsync(MetaStorageManager::get)
                )),
                Arguments.of(Named.named(
                        "getSingleBounded",
                        ReadFromLeaderAction.readAsync((metastore, key) -> metastore.get(key, 2))
                )),
                Arguments.of(Named.named(
                        "getAllLatest",
                        ReadFromLeaderAction.readAsync((metastore, key) -> metastore.getAll(Set.of(key)))
                )),
                Arguments.of(Named.named(
                        "prefixLatest",
                        ReadFromLeaderAction.readAsync((metastore, key) -> subscribeToList(metastore.prefix(key)))
                )),
                Arguments.of(Named.named(
                        "prefixBounded",
                        ReadFromLeaderAction.readAsync((metastore, key) -> subscribeToList(metastore.prefix(key, 2)))
                ))
        );
    }

    private void listenReadActionRequest(
            CompletableFuture<Void> startSendReadActionRequestFuture,
            CompletableFuture<Void> continueSendReadActionRequestFuture
    ) {
        ((DefaultMessagingService) clusterService.messagingService()).dropMessages((recipientConsistentId, message) -> {
            if (message instanceof ReadActionRequest) {
                startSendReadActionRequestFuture.complete(null);

                assertThat(continueSendReadActionRequestFuture, willCompleteSuccessfully());
            }

            return false;
        });
    }

    private static CompletableFuture<Void> waitFor(int timeout, TimeUnit unit) {
        return new CompletableFuture<Void>()
                .orTimeout(timeout, unit)
                .exceptionally(ex -> null);
    }
}
