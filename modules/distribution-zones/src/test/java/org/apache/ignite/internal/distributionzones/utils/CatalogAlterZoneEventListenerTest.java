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

package org.apache.ignite.internal.distributionzones.utils;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.alterZoneBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createZoneBuilder;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.BaseCatalogManagerTest;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** For {@link CatalogAlterZoneEventListener} testing. */
public class CatalogAlterZoneEventListenerTest extends BaseCatalogManagerTest {
    private static final String ZONE_NAME = "test_zone";

    @Test
    void testOnUpdateZone() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22374")
    void testOnUpdatePartitions() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onPartitionsUpdateFuture = new CompletableFuture<>();

        int newPartitions = 101;

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onPartitionsUpdate(AlterZoneEventParameters parameters, int oldPartitions) {
                assertNotEquals(newPartitions, oldPartitions);

                onPartitionsUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).partitions(newPartitions).build()), willCompleteSuccessfully());

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onPartitionsUpdateFuture, willCompleteSuccessfully());
    }


    @Test
    void testOnUpdateReplicas() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onReplicasUpdateFuture = new CompletableFuture<>();

        int newReplicas = 202;

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
                assertNotEquals(newReplicas, oldReplicas);

                onReplicasUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).replicas(newReplicas).build()), willCompleteSuccessfully());

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onReplicasUpdateFuture, willCompleteSuccessfully());
    }

    @Test
    void testOnUpdateFilter() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onFilterUpdateFuture = new CompletableFuture<>();

        String newFilter = "['nodeAttributes'][?(@.['region'] == 'EU')]";

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
                assertNotEquals(newFilter, oldFilter);

                onFilterUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).filter(newFilter).build()), willCompleteSuccessfully());

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onFilterUpdateFuture, willCompleteSuccessfully());
    }

    @Test
    void testOnUpdateAutoAdjust() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onAutoAdjustUpdateFuture = new CompletableFuture<>();

        int newAutoAdjust = 303;

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onAutoAdjustUpdate(AlterZoneEventParameters parameters, int oldAutoAdjust) {
                assertNotEquals(newAutoAdjust, oldAutoAdjust);

                onAutoAdjustUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjust(newAutoAdjust).build()), willCompleteSuccessfully());

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onAutoAdjustUpdateFuture, willCompleteSuccessfully());
    }

    @Test
    void testOnUpdateAutoAdjustScaleUp() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onAutoAdjustScaleUpUpdateFuture = new CompletableFuture<>();

        int newAutoAdjustScaleUp = 404;

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
                assertNotEquals(newAutoAdjustScaleUp, oldAutoAdjustScaleUp);

                onAutoAdjustScaleUpUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleUp(newAutoAdjustScaleUp).build()),
                willCompleteSuccessfully()
        );

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onAutoAdjustScaleUpUpdateFuture, willCompleteSuccessfully());
    }

    @Test
    void testOnUpdateAutoAdjustScaleDown() {
        CompletableFuture<Void> onZoneUpdateFuture = new CompletableFuture<>();
        CompletableFuture<Void> onAutoAdjustScaleDownUpdateFuture = new CompletableFuture<>();

        int newAutoAdjustScaleDown = 505;

        listenAlterZone(new CatalogAlterZoneEventListener(manager) {
            @Override
            protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
                onZoneUpdateFuture.complete(null);

                return nullCompletedFuture();
            }

            @Override
            protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
                assertNotEquals(newAutoAdjustScaleDown, oldAutoAdjustScaleDown);

                onAutoAdjustScaleDownUpdateFuture.complete(null);

                return nullCompletedFuture();
            }
        });

        assertThat(manager.execute(createZoneBuilder(ZONE_NAME).build()), willCompleteSuccessfully());
        assertThat(
                manager.execute(alterZoneBuilder(ZONE_NAME).dataNodesAutoAdjustScaleDown(newAutoAdjustScaleDown).build()),
                willCompleteSuccessfully()
        );

        assertThat(onZoneUpdateFuture, willCompleteSuccessfully());
        assertThat(onAutoAdjustScaleDownUpdateFuture, willCompleteSuccessfully());
    }

    private void listenAlterZone(CatalogAlterZoneEventListener listener) {
        manager.listen(ZONE_ALTER, listener);
    }
}
