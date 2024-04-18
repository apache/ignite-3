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

package org.apache.ignite.internal.systemview;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.systemview.SystemViewManagerImpl.NODE_ATTRIBUTES_KEY;
import static org.apache.ignite.internal.systemview.SystemViewManagerImpl.NODE_ATTRIBUTES_LIST_SEPARATOR;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.SubscriptionUtils.fromIterable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link SystemViewManagerImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class SystemViewManagerTest extends BaseIgniteAbstractTest {
    private static final String LOCAL_NODE_NAME = "LOCAL_NODE_NAME";

    @Mock
    private CatalogManager catalog;

    private SystemViewManagerImpl viewMgr;

    @BeforeEach
    void setUp() {
        viewMgr = new SystemViewManagerImpl(LOCAL_NODE_NAME, catalog);
    }

    @Test
    public void registerDuplicateNameFails() {
        String name = "testView1";

        SystemView<?> view = dummyView(name);
        SystemView<?> viewWithSameName = dummyView(name);

        viewMgr.register(() -> List.of(view));

        assertThrows(IllegalArgumentException.class, () -> viewMgr.register(() -> List.of(viewWithSameName)));
        verifyNoInteractions(catalog);
    }

    @Test
    public void registerAfterStartFails() {
        viewMgr.startAsync();

        assertThrows(IllegalStateException.class, () -> viewMgr.register(() -> List.of(dummyView("test"))));
        verifyNoInteractions(catalog);
    }

    @Test
    public void startAfterStartFails() {
        Mockito.when(catalog.execute(anyList())).thenReturn(nullCompletedFuture());

        viewMgr.register(() -> List.of(dummyView("test")));

        viewMgr.startAsync();

        verify(catalog, only()).execute(anyList());

        assertThrows(IllegalStateException.class, viewMgr::startAsync);

        verifyNoMoreInteractions(catalog);
    }

    @Test
    public void registrationCompletesWithoutViews() {
        viewMgr.startAsync();

        verifyNoMoreInteractions(catalog);

        assertTrue(viewMgr.completeRegistration().isDone());
    }

    @ParameterizedTest
    @EnumSource(NativeTypeSpec.class)
    public void registerAllColumnTypes(NativeTypeSpec typeSpec) {
        NativeType type = SchemaTestUtils.specToType(typeSpec);

        Mockito.when(catalog.execute(anyList())).thenReturn(nullCompletedFuture());

        viewMgr.register(() -> List.of(dummyView("test", type)));
        viewMgr.startAsync();

        verify(catalog, only()).execute(anyList());
        assertTrue(viewMgr.completeRegistration().isDone());
    }

    @Test
    public void managerStartsSuccessfullyEvenIfCatalogRespondsWithError() {
        CatalogValidationException expected = new CatalogValidationException("Expected exception.");

        Mockito.when(catalog.execute(anyList())).thenReturn(failedFuture(expected));

        viewMgr.register(() -> List.of(dummyView("test")));

        viewMgr.startAsync();

        verify(catalog, only()).execute(anyList());

        assertThat(viewMgr.completeRegistration(), willBe(nullValue()));
    }

    @Test
    public void nodeAttributesUpdatedAfterStart() {
        Mockito.when(catalog.execute(anyList())).thenReturn(nullCompletedFuture());

        String name1 = "view1";
        String name2 = "view2";

        viewMgr.register(() -> List.of(dummyView(name1), dummyView(name2)));

        assertThat(viewMgr.nodeAttributes(), aMapWithSize(0));

        viewMgr.startAsync();

        verify(catalog, only()).execute(anyList());
        verifyNoMoreInteractions(catalog);

        assertThat(viewMgr.nodeAttributes(), is(Map.of(NODE_ATTRIBUTES_KEY, String.join(NODE_ATTRIBUTES_LIST_SEPARATOR, name1.toUpperCase(
                Locale.ROOT), name2.toUpperCase(Locale.ROOT)))));
    }

    @Test
    public void registrationFutureCompletesWhenComponentStops() throws Exception {
        viewMgr.stopAsync();

        assertThat(viewMgr.completeRegistration(), willThrowFast(NodeStoppingException.class));
    }

    @Test
    public void startAfterStopFails() throws Exception {
        viewMgr.stopAsync();

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(viewMgr::startAsync, NodeStoppingException.class);
    }

    @Test
    public void registerAfterStopFails() throws Exception {
        viewMgr.stopAsync();

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(() -> viewMgr.register(() -> List.of(dummyView("test"))), NodeStoppingException.class);
    }

    @Test
    public void stopAfterStopDoesNothing() throws Exception {
        viewMgr.stopAsync();
        viewMgr.stopAsync();
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void managerDerivesViewPlacementFromLogicalTopologyEvents() {
        String viewName = "MY_VIEW";

        assertThat(viewMgr.owningNodes(viewName), empty());

        List<String> allNodes = List.of("A", "B", "C");

        LogicalTopologySnapshot topologySnapshot = topologySnapshot(viewName, allNodes, 0, 2);
        viewMgr.onNodeJoined(first(topologySnapshot.nodes()), topologySnapshot);

        assertThat(viewMgr.owningNodes(viewName), hasItem("A"));
        assertThat(viewMgr.owningNodes(viewName), hasItem("C"));

        topologySnapshot = topologySnapshot(viewName, allNodes, 0, 1);
        viewMgr.onNodeLeft(first(topologySnapshot.nodes()), topologySnapshot);

        assertThat(viewMgr.owningNodes(viewName), hasItem("A"));
        assertThat(viewMgr.owningNodes(viewName), hasItem("B"));

        topologySnapshot = topologySnapshot(viewName, allNodes, 1, 2);
        viewMgr.onTopologyLeap(topologySnapshot);

        assertThat(viewMgr.owningNodes(viewName), hasItem("B"));
        assertThat(viewMgr.owningNodes(viewName), hasItem("C"));
    }

    @Test
    void viewScanTest() {
        Mockito.when(catalog.execute(anyList())).thenReturn(nullCompletedFuture());

        String nodeView = "NODE_VIEW";
        String clusterView = "CLUSTER_VIEW";

        class Pojo {
            private final int c1;
            private final int c2;

            private Pojo(int c1, int c2) {
                this.c1 = c1;
                this.c2 = c2;
            }
        }

        Publisher<Pojo> dataSet = fromIterable(List.of(new Pojo(1, 1), new Pojo(2, 2)));

        viewMgr.register(() -> List.of(
                SystemViews.<Pojo>nodeViewBuilder()
                        .name(nodeView)
                        .nodeNameColumnAlias("NODE")
                        .addColumn("C1", NativeTypes.INT32, p -> p.c1)
                        .addColumn("C2", NativeTypes.INT32, p -> p.c2)
                        .dataProvider(dataSet)
                        .build(),
                SystemViews.<Pojo>clusterViewBuilder()
                        .name(clusterView)
                        .addColumn("C1", NativeTypes.INT32, p -> p.c1)
                        .addColumn("C2", NativeTypes.INT32, p -> p.c2)
                        .dataProvider(dataSet)
                        .build()
        ));

        viewMgr.startAsync();

        {
            DrainAllSubscriber<InternalTuple> subs = new DrainAllSubscriber<>();

            viewMgr.scanView(clusterView).subscribe(subs);

            List<InternalTuple> entries = await(subs.completion);

            assertThat(entries, hasSize(2));
            assertThat(entries.get(0).intValue(0), equalTo(1));
            assertThat(entries.get(0).intValue(1), equalTo(1));
            assertThat(entries.get(1).intValue(0), equalTo(2));
            assertThat(entries.get(1).intValue(1), equalTo(2));
        }

        {
            DrainAllSubscriber<InternalTuple> subs = new DrainAllSubscriber<>();

            viewMgr.scanView(nodeView).subscribe(subs);

            List<InternalTuple> entries = await(subs.completion);

            assertThat(entries, hasSize(2));
            assertThat(entries.get(0).stringValue(0), equalTo(LOCAL_NODE_NAME));
            assertThat(entries.get(0).intValue(1), equalTo(1));
            assertThat(entries.get(0).intValue(2), equalTo(1));
            assertThat(entries.get(1).stringValue(0), equalTo(LOCAL_NODE_NAME));
            assertThat(entries.get(1).intValue(1), equalTo(2));
            assertThat(entries.get(1).intValue(2), equalTo(2));
        }

    }

    private static SystemView<?> dummyView(String name) {
        return dummyView(name, NativeTypes.INT32);
    }

    private static <T> SystemView<T> dummyView(String name, NativeType type) {
        return (SystemView<T>) SystemViews.nodeViewBuilder()
                .nodeNameColumnAlias("NODE")
                .name(name)
                .addColumn("c1", type, (Function<Object, T>) Function.identity())
                .dataProvider(fromIterable(List.of()))
                .build();
    }

    private static LogicalTopologySnapshot topologySnapshot(String viewName, List<String> allNodes, int... owningNodes) {
        BitSet owningNodesSet = new BitSet();

        for (int idx : owningNodes) {
            owningNodesSet.set(idx);
        }

        List<LogicalNode> topology = new ArrayList<>(allNodes.size());

        for (int i = 0; i < allNodes.size(); i++) {
            String name = allNodes.get(i);

            ClusterNode clusterNode = new ClusterNodeImpl(name, name, new NetworkAddress("127.0.0.1", 1010 + i));

            Map<String, String> systemAttributes;
            if (owningNodesSet.get(i)) {
                systemAttributes = Map.of(NODE_ATTRIBUTES_KEY, viewName);
            } else {
                systemAttributes = Map.of();
            }

            topology.add(new LogicalNode(clusterNode, Map.of(), systemAttributes, List.of()));
        }

        return new LogicalTopologySnapshot(1, topology);
    }

    static class DrainAllSubscriber<T> implements Subscriber<T> {
        private final List<T> entries = new ArrayList<>();

        CompletableFuture<List<T>> completion = new CompletableFuture<>();

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            entries.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            completion.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            completion.complete(entries);
        }
    }
}
