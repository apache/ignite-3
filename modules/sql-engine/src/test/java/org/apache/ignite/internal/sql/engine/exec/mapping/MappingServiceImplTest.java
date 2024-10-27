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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignmentsImpl;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.engine.ExecutionDistributionProvider;
import org.apache.ignite.internal.sql.engine.ExecutionDistributionProviderImpl.DistributionHolder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class to verify {@link MappingServiceImpl}.
 */
@SuppressWarnings("ThrowFromFinallyBlock")
public class MappingServiceImplTest extends BaseIgniteAbstractTest {
    private static final MultiStepPlan PLAN;
    private static final MultiStepPlan PLAN_WITH_SYSTEM_VIEW;
    private static final TestCluster cluster;
    private static final ClockService CLOCK_SERVICE = new TestClockService(new TestHybridClock(System::currentTimeMillis));
    private static final MappingParameters PARAMS = MappingParameters.EMPTY;
    private static final PartitionPruner PARTITION_PRUNER = (fragments, dynParams) -> fragments;
    private long topologyVer;
    private boolean topologyChange;

    static {
        // @formatter:off
        cluster = TestBuilders.cluster()
                .nodes("N1")
                .addTable()
                        .name("T1")
                        .addKeyColumn("ID", NativeTypes.INT32)
                        .addColumn("VAL", NativeTypes.INT32)
                        .end()
                .addTable()
                        .name("T2")
                        .addKeyColumn("ID", NativeTypes.INT32)
                        .addColumn("VAL", NativeTypes.INT32)
                        .end()
                .addSystemView(SystemViews.<Object[]>clusterViewBuilder()
                        .name("TEST_VIEW")
                        .addColumn("ID", NativeTypes.INT64, v -> v[0])
                        .addColumn("WORD", NativeTypes.stringOf(64), v -> v[1])
                        .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42L, "blah"})))
                        .build())
                .build();
        // @formatter:on

        cluster.start();

        try {
            PLAN = (MultiStepPlan) cluster.node("N1").prepare("SELECT * FROM t1");
            PLAN_WITH_SYSTEM_VIEW = (MultiStepPlan) cluster.node("N1").prepare("SELECT * FROM system.test_view, t1");
        } finally {
            try {
                cluster.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void cacheOnStableTopology() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        // Initialize mapping service.
        MappingServiceImpl mappingService = Mockito.spy(createMappingService(localNodeName, nodeNames, 100));

        List<MappedFragment> defaultMapping = await(mappingService.map(PLAN, PARAMS));
        List<MappedFragment> mappingOnBackups = await(mappingService.map(PLAN, MappingParameters.MAP_ON_BACKUPS));

        verify(mappingService, times(2)).forTable(any(), any());

        assertSame(defaultMapping, await(mappingService.map(PLAN, PARAMS)));
        assertSame(mappingOnBackups, await(mappingService.map(PLAN, MappingParameters.MAP_ON_BACKUPS)));
        assertNotSame(defaultMapping, mappingOnBackups);
    }

    @Test
    public void serviceInitializationTest() {
        String localNodeName = "NODE0";

        MappingServiceImpl mappingService = createMappingServiceNoCache(localNodeName, List.of(localNodeName));

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService.map(PLAN, PARAMS);

        assertThat(mappingFuture, willSucceedFast());
    }

    @Test
    public void testCacheInvalidationOnTopologyChange() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        Supplier<LogicalTopologySnapshot> logicalTopologySupplier = createTriggeredTopologySupplier();
        TestExecutionDistributionProvider execProvider = Mockito.spy(new TestExecutionDistributionProvider(nodeNames));
        SystemViewManager viewManager = Mockito.mock(SystemViewManager.class);
        when(viewManager.owningNodes(any())).thenReturn(nodeNames);

        MappingServiceImpl mappingService = Mockito.spy(new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                100,
                PARTITION_PRUNER,
                logicalTopologySupplier,
                viewManager,
                execProvider
        ));

        List<MappedFragment> tableOnlyMapping = await(mappingService.map(PLAN, PARAMS));
        List<MappedFragment> sysViewMapping = await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));

        verify(mappingService, times(2)).forTable(any(), any());
        verify(mappingService, times(1)).forSystemView(any(), any(), any());

        verify(execProvider, times(2)).distribution(any(HybridTimestamp.class), anyBoolean(),
                anyCollection(), anyString());

        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));
        assertSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        verifyNoMoreInteractions(execProvider);

        topologyChange = true;

        // Plan with system views must be invalidated.
        assertNotSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        // Plan with tables only must not be invalidated on topology change.
        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));

        verify(mappingService, times(3)).forTable(any(), any());
        verify(mappingService, times(2)).forSystemView(any(), any(), any());
    }

    @Test
    public void testCacheInvalidationOnPrimaryExpiration() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        Function<String, PrimaryReplicaEventParameters> prepareEvtParams = (name) -> {
            CatalogService catalogService = cluster.catalogManager();
            Catalog catalog = catalogService.catalog(catalogService.latestCatalogVersion());

            Optional<Integer> tblId = catalog.tables().stream()
                    .filter(desc -> name.equals(desc.name()))
                    .findFirst()
                    .map(CatalogObjectDescriptor::id);

            assertTrue(tblId.isPresent());

            return new PrimaryReplicaEventParameters(
                    0, new TablePartitionId(tblId.get(), 0), new UUID(0, 0), "ignored", HybridTimestamp.MIN_VALUE);
        };

        // Initialize mapping service.
        Supplier<LogicalTopologySnapshot> logicalTopologySupplier = createStableTopologySupplier();
        ExecutionDistributionProvider execProvider = Mockito.spy(new TestExecutionDistributionProvider(nodeNames));

        SystemViewManager viewManager = Mockito.mock(SystemViewManager.class);
        when(viewManager.owningNodes(any())).thenReturn(nodeNames);

        MappingServiceImpl mappingService = Mockito.spy(new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                100,
                PARTITION_PRUNER,
                logicalTopologySupplier,
                viewManager,
                execProvider
        ));

        List<MappedFragment> mappedFragments = await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));
        verify(mappingService, times(1)).forTable(any(), any());
        verify(mappingService, times(1)).forSystemView(any(), any(), any());

        // Simulate expiration of the primary replica for non-mapped table - the cache entry should not be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply("T2")));
        assertSame(mappedFragments, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        verify(execProvider, times(1)).distribution(any(HybridTimestamp.class), anyBoolean(),
                anyCollection(), anyString());

        // Simulate expiration of the primary replica for mapped table - the cache entry should be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply("T1")));
        assertNotSame(mappedFragments, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));
        verify(mappingService, times(2)).forTable(any(), any());
        verify(mappingService, times(2)).forSystemView(any(), any(), any());
    }

    private MappingServiceImpl createMappingServiceNoCache(String localNodeName, List<String> nodeNames) {
        return createMappingService(localNodeName, nodeNames, 0);
    }

    private MappingServiceImpl createMappingService(String localNodeName, List<String> nodeNames, int cacheSize) {
        Supplier<LogicalTopologySnapshot> logicalTopologySupplier = createChangingTopologySupplier();
        ExecutionDistributionProvider execProvider = new TestExecutionDistributionProvider(nodeNames);

        return new MappingServiceImpl(
                localNodeName,
                CLOCK_SERVICE,
                CaffeineCacheFactory.INSTANCE,
                cacheSize,
                PARTITION_PRUNER,
                logicalTopologySupplier,
                null,
                execProvider
        );
    }

    /** Test distribution provider. */
    public static class TestExecutionDistributionProvider implements ExecutionDistributionProvider {
        private final List<String> nodeNames;
        private Supplier<RuntimeException> exceptionSupplier = () -> null;

        TestExecutionDistributionProvider(List<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        /** Constructor. */
        public TestExecutionDistributionProvider(List<String> nodeNames, Supplier<RuntimeException> exceptionSupplier) {
            this.nodeNames = nodeNames;
            this.exceptionSupplier = exceptionSupplier;
        }

        @Override
        public CompletableFuture<DistributionHolder> distribution(
                HybridTimestamp operationTime,
                boolean mapOnBackups,
                Collection<IgniteTable> tables,
                String initiatorNode
        ) {
            DistributionHolder holder = new DistributionHolder() {
                @Override
                public List<String> nodes() {
                    return nodeNames;
                }

                private TokenizedAssignments mapAssignment(String peer) {
                    Set<Assignment> peers = Set.of(Assignment.forPeer(peer));
                    return new TokenizedAssignmentsImpl(peers, 1L);
                }

                @Override
                public List<TokenizedAssignments> assignmentsPerTable(IgniteTable table) {
                    if (exceptionSupplier.get() != null) {
                        throw exceptionSupplier.get();
                    }

                    return nodeNames.stream().map(this::mapAssignment).collect(Collectors.toList());
                }
            };

            return CompletableFuture.completedFuture(holder);
        }
    }

    private static Supplier<LogicalTopologySnapshot> createStableTopologySupplier() {
        return () -> new LogicalTopologySnapshot(1, List.of(), randomUUID());
    }

    private Supplier<LogicalTopologySnapshot> createTriggeredTopologySupplier() {
        return () -> new LogicalTopologySnapshot(topologyChange ? ++topologyVer : topologyVer, List.of(), randomUUID());
    }

    private Supplier<LogicalTopologySnapshot> createChangingTopologySupplier() {
        return () -> new LogicalTopologySnapshot(topologyVer++, List.of(), randomUUID());
    }
}
