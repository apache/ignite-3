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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruner;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.EmptyCacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.network.NetworkAddress;
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
    private static final MappingParameters PARAMS = MappingParameters.EMPTY;
    private static final PartitionPruner PARTITION_PRUNER = (fragments, dynParams) -> fragments;

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
    public void serviceInitializationTest() {
        String localNodeName = "NODE0";

        MappingServiceImpl mappingService = createMappingServiceNoCache(localNodeName, List.of(localNodeName));
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class), new LogicalTopologySnapshot(1, logicalNodes(localNodeName)));

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService.map(PLAN, PARAMS);

        assertThat(mappingFuture, willSucceedFast());
    }

    @Test
    public void lateServiceInitializationOnTopologyLeap() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of("NODE1");

        MappingServiceImpl mappingService = createMappingServiceNoCache(localNodeName, nodeNames);

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService.map(PLAN, PARAMS);

        // Mapping should wait for service initialization.
        assertFalse(mappingFuture.isDone());

        // Join another node affect nothing.
        mappingService.onTopologyLeap(new LogicalTopologySnapshot(1, logicalNodes("NODE1", "NODE2")));
        assertThat(mappingFuture, willThrowFast(TimeoutException.class));

        // Joining local node completes initialization.
        mappingService.onTopologyLeap(new LogicalTopologySnapshot(2, logicalNodes("NODE", "NODE1", "NODE2")));

        assertThat(mappingFuture, willSucceedFast());
        assertThat(mappingService.map(PLAN, PARAMS), willSucceedFast());
    }

    @Test
    public void lateServiceInitializationOnNodeJoin() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of("NODE1");

        MappingServiceImpl mappingService = createMappingServiceNoCache(localNodeName, nodeNames);

        CompletableFuture<List<MappedFragment>> mappingFuture = mappingService.map(PLAN, PARAMS);

        // Mapping should wait for service initialization.
        assertFalse(mappingFuture.isDone());

        // Join another node affect nothing.
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(1, logicalNodes("NODE1", "NODE2")));

        assertThat(mappingFuture, willThrowFast(TimeoutException.class));

        // Joining local node completes initialization.
        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(2, logicalNodes("NODE", "NODE1", "NODE2")));

        assertThat(mappingFuture, willSucceedFast());
        assertThat(mappingService.map(PLAN, PARAMS), willSucceedFast());
    }

    @Test
    public void testCacheInvalidationOnTopologyChange() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        // Initialize mapping service.
        ExecutionTargetProvider targetProvider = Mockito.spy(createTargetProvider(nodeNames));
        MappingServiceImpl mappingService =
                new MappingServiceImpl(localNodeName, targetProvider, CaffeineCacheFactory.INSTANCE, 100, PARTITION_PRUNER, Runnable::run);

        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(1, logicalNodes(nodeNames.toArray(new String[0]))));

        List<MappedFragment> tableOnlyMapping = await(mappingService.map(PLAN, PARAMS));
        List<MappedFragment> sysViewMapping = await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));

        verify(targetProvider, times(2)).forTable(any(), any());
        verify(targetProvider, times(1)).forSystemView(any(), any());

        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));
        assertSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        LogicalNode newNode = Mockito.mock(LogicalNode.class);
        Mockito.when(newNode.name()).thenReturn("NODE2");

        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(3, logicalNodes("NODE", "NODE1", "NODE2")));

        // Plan with tables only must not be invalidated on node join.
        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));
        verifyNoMoreInteractions(targetProvider);

        // Plan with system views must be invalidated.
        assertNotSame(sysViewMapping, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));

        mappingService.onNodeLeft(newNode,
                new LogicalTopologySnapshot(3, logicalNodes("NODE", "NODE1")));

        // Plan with tables that don't include a left node should not be invalidated.
        assertSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));

        LogicalNode node1 = Mockito.mock(LogicalNode.class);
        Mockito.when(node1.name()).thenReturn("NODE1");

        mappingService.onNodeLeft(node1,
                new LogicalTopologySnapshot(3, logicalNodes("NODE")));

        // Plan with tables that include left node must be invalidated.
        assertNotSame(tableOnlyMapping, await(mappingService.map(PLAN, PARAMS)));

        verify(targetProvider, times(4)).forTable(any(), any());
        verify(targetProvider, times(2)).forSystemView(any(), any());
    }

    @Test
    public void testCacheInvalidationOnPrimaryExpiration() {
        String localNodeName = "NODE";
        List<String> nodeNames = List.of(localNodeName, "NODE1");

        Function<String, PrimaryReplicaEventParameters> prepareEvtParams = (name) -> {
            CatalogService catalogService = cluster.catalogManager();
            Catalog catalog = catalogService.catalog(catalogService.latestCatalogVersion());

            Optional<CatalogTableDescriptor> tblDesc = catalog.tables().stream()
                    .filter(desc -> name.equals(desc.name()))
                    .findFirst();

            assertTrue(tblDesc.isPresent());

            return new PrimaryReplicaEventParameters(
                    0, new ZonePartitionId(tblDesc.get().zoneId(), tblDesc.get().id(), 0), "ignored", "ignored", HybridTimestamp.MIN_VALUE);
        };

        // Initialize mapping service.
        ExecutionTargetProvider targetProvider = Mockito.spy(createTargetProvider(nodeNames));
        MappingServiceImpl mappingService =
                new MappingServiceImpl(localNodeName, targetProvider, CaffeineCacheFactory.INSTANCE, 100, PARTITION_PRUNER, Runnable::run);

        mappingService.onNodeJoined(Mockito.mock(LogicalNode.class),
                new LogicalTopologySnapshot(1, logicalNodes(nodeNames.toArray(new String[0]))));

        List<MappedFragment> mappedFragments = await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS));
        verify(targetProvider, times(1)).forTable(any(), any());
        verify(targetProvider, times(1)).forSystemView(any(), any());

        // Simulate expiration of the primary replica for non-mapped table - the cache entry should not be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply("T2")));
        assertSame(mappedFragments, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));
        verifyNoMoreInteractions(targetProvider);

        // Simulate expiration of the primary replica for mapped table - the cache entry should be invalidated.
        await(mappingService.onPrimaryReplicaExpired(prepareEvtParams.apply("T1")));
        assertNotSame(mappedFragments, await(mappingService.map(PLAN_WITH_SYSTEM_VIEW, PARAMS)));
        verify(targetProvider, times(2)).forTable(any(), any());
        verify(targetProvider, times(2)).forSystemView(any(), any());
    }

    private static List<LogicalNode> logicalNodes(String... nodeNames) {
        return Arrays.stream(nodeNames)
                .map(name -> new LogicalNode(name, name, NetworkAddress.from("127.0.0.1:10000")))
                .collect(Collectors.toList());
    }

    private static MappingServiceImpl createMappingServiceNoCache(String localNodeName, List<String> nodeNames) {
        return new MappingServiceImpl(
                localNodeName,
                createTargetProvider(nodeNames),
                EmptyCacheFactory.INSTANCE,
                0,
                PARTITION_PRUNER,
                Runnable::run
        );
    }

    private static ExecutionTargetProvider createTargetProvider(List<String> nodeNames) {
        return new ExecutionTargetProvider() {
            @Override
            public CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, IgniteTable table) {
                return CompletableFuture.completedFuture(factory.allOf(nodeNames));
            }

            @Override
            public CompletableFuture<ExecutionTarget> forSystemView(ExecutionTargetFactory factory, IgniteSystemView view) {
                return CompletableFuture.completedFuture(view.distribution() == IgniteDistributions.single()
                        ? factory.oneOf(nodeNames)
                        : factory.allOf(nodeNames));
            }
        };
    }
}
