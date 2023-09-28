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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.systemview.SystemViewManagerImpl.NODE_ATTRIBUTES_KEY;
import static org.apache.ignite.internal.systemview.SystemViewManagerImpl.NODE_ATTRIBUTES_LIST_SEPARATOR;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.AsyncCursor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link SystemViewManagerImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class SystemViewManagerTest extends BaseIgniteAbstractTest {
    @Mock
    private CatalogManager catalog;

    @InjectMocks
    private SystemViewManagerImpl viewMgr;

    @Test
    public void registerDuplicateNameFails() {
        String name = "testView1";

        ClusterSystemView<Object> view = dummyView(name);
        ClusterSystemView<Object> viewWithSameName = dummyView(name);

        viewMgr.register(view);

        assertThrows(IllegalArgumentException.class, () -> viewMgr.register(viewWithSameName));
        verifyNoInteractions(catalog);
    }

    @Test
    public void registerAfterStartFails() {
        viewMgr.start();

        assertThrows(IllegalStateException.class, () -> viewMgr.register(dummyView("test")));
        verifyNoInteractions(catalog);
    }

    @Test
    public void startAfterStartFails() {
        Mockito.when(catalog.execute(anyList())).thenReturn(completedFuture(null));

        viewMgr.register(dummyView("test"));

        viewMgr.start();

        verify(catalog, only()).execute(anyList());

        assertThrows(IllegalStateException.class, viewMgr::start);

        verifyNoMoreInteractions(catalog);
    }

    @Test
    public void registrationCompletesWithoutViews() {
        viewMgr.start();

        verifyNoMoreInteractions(catalog);

        assertTrue(viewMgr.completeRegistration().isDone());
    }

    @Test
    public void managerStartsSuccessfullyEvenIfCatalogRespondsWithError() {
        CatalogValidationException expected = new CatalogValidationException("Expected exception.");

        Mockito.when(catalog.execute(anyList())).thenReturn(failedFuture(expected));

        viewMgr.register(dummyView("test"));

        viewMgr.start();

        verify(catalog, only()).execute(anyList());

        assertThat(viewMgr.completeRegistration(), willBe(nullValue()));
    }

    @Test
    public void nodeAttributesUpdatedAfterStart() {
        Mockito.when(catalog.execute(anyList())).thenReturn(completedFuture(null));

        String name1 = "view1";
        String name2 = "view2";

        viewMgr.register(dummyView(name1));
        viewMgr.register(dummyView(name2));

        assertThat(viewMgr.nodeAttributes(), aMapWithSize(0));

        viewMgr.start();

        verify(catalog, only()).execute(anyList());
        verifyNoMoreInteractions(catalog);

        assertThat(viewMgr.nodeAttributes(), is(Map.of(NODE_ATTRIBUTES_KEY, String.join(NODE_ATTRIBUTES_LIST_SEPARATOR, name1, name2))));
    }

    @Test
    public void registrationFutureCompletesWhenComponentStops() throws Exception {
        viewMgr.stop();

        assertThat(viewMgr.completeRegistration(), willThrowFast(NodeStoppingException.class));
    }

    @Test
    public void startAfterStopFails() throws Exception {
        viewMgr.stop();

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(viewMgr::start, NodeStoppingException.class);
    }

    @Test
    public void registerAfterStopFails() throws Exception {
        viewMgr.stop();

        //noinspection ThrowableNotThrown
        assertThrowsWithCause(() -> viewMgr.register(dummyView("test")), NodeStoppingException.class);
    }

    @Test
    public void stopAfterStopDoesNothing() throws Exception {
        viewMgr.stop();
        viewMgr.stop();
    }

    private static ClusterSystemView<Object> dummyView(String name) {
        return SystemViews.clusterViewBuilder().name(name)
                .addColumn("c1", int.class, (d) -> 1)
                .dataProvider(() -> new AsyncCursor<>() {
                    @Override
                    public CompletableFuture<BatchedResult<Object>> requestNextAsync(int rows) {
                        return completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        return completedFuture(null);
                    }
                })
                .build();
    }
}
