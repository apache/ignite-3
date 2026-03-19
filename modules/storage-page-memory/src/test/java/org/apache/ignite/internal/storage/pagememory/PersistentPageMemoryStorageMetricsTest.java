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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link PersistentPageMemoryStorageMetrics} testing. */
public class PersistentPageMemoryStorageMetricsTest extends BaseIgniteAbstractTest {
    private final MetricManager metricManager = new TestMetricManager();

    private final CollectionMetricSource metricSource = new CollectionMetricSource("test", "storage", null);

    private final FilePageStoreManager filePageStoreManager = mock(FilePageStoreManager.class);

    @BeforeEach
    void setUp() {
        PersistentPageMemoryStorageMetrics.initMetrics(metricSource, filePageStoreManager);

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        metricManager.beforeNodeStop();

        assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testStorageSize() throws Exception {
        checkMetricValue("StorageSize", "0");

        setAllPageStores(
                pageStore(new GroupPartitionId(1, 1), 100L),
                pageStore(new GroupPartitionId(1, 2), 200L),
                pageStoreWithException(new GroupPartitionId(2, 1))
        );

        checkMetricValue("StorageSize", "300");
    }

    @SafeVarargs
    private void setAllPageStores(GroupPartitionPageStore<FilePageStore>... pageStores) {
        Stream<GroupPartitionPageStore<FilePageStore>> stream = Arrays.stream(pageStores);

        when(filePageStoreManager.allPageStores()).thenReturn(stream);
    }

    private static GroupPartitionPageStore<FilePageStore> pageStore(GroupPartitionId groupPartitionId, long size) throws Exception {
        FilePageStore pageStore = mock(FilePageStore.class);

        when(pageStore.fullSize()).thenReturn(size);

        return new GroupPartitionPageStore<>(groupPartitionId, pageStore);
    }

    private static GroupPartitionPageStore<FilePageStore> pageStoreWithException(GroupPartitionId groupPartitionId) throws Exception {
        FilePageStore pageStore = mock(FilePageStore.class);

        when(pageStore.fullSize()).thenThrow(IgniteInternalCheckedException.class);

        return new GroupPartitionPageStore<>(groupPartitionId, pageStore);
    }

    private void checkMetricValue(String metricName, String exp) {
        MetricSet metricSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        Metric metric = metricSet.get(metricName);

        assertNotNull(metric, metricName);

        assertEquals(exp, metric.getValueAsString(), metricName);
    }
}
