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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.event.EventListener;

/**
 * Event listener for changing the distribution zone and its fields.
 *
 * <p>To listen for changes to the distribution zone and / or any of the fields, you need to override the methods:</p>
 * <ul>
 *     <li>{@link #onZoneUpdate(AlterZoneEventParameters, CatalogZoneDescriptor)};</li>
 *     <li>{@link #onPartitionsUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onReplicasUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onFilterUpdate(AlterZoneEventParameters, String)};</li>
 *     <li>{@link #onAutoAdjustUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onAutoAdjustScaleUpUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onAutoAdjustScaleDownUpdate(AlterZoneEventParameters, int)}.</li>
 * </ul>
 *
 * <p>For example, if you change the count of partitions and replicas for a zone, the following methods will be called:
 * {@link #onZoneUpdate(AlterZoneEventParameters, CatalogZoneDescriptor) onZoneUpdate},
 * {@link #onPartitionsUpdate(AlterZoneEventParameters, int) onPartitionsUpdate} and
 * {@link #onReplicasUpdate(AlterZoneEventParameters, int) onReplicasUpdate} (order of calling methods may change in the future).</p>
 *
 * <p>If you need more complex processing, such as calling a listener when only the count of partitions and replicas (or one of them) has
 * changed, then you need to either override {@link #onZoneUpdate(AlterZoneEventParameters, CatalogZoneDescriptor) onZoneUpdate} or write
 * your own implementation of {@link EventListener}.</p>
 */
public class CatalogAlterZoneEventListener implements EventListener<AlterZoneEventParameters> {
    private final CatalogService catalogService;

    /**
     * Constructor.
     *
     * @param catalogService Catalog service.
     */
    public CatalogAlterZoneEventListener(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public CompletableFuture<Boolean> notify(AlterZoneEventParameters parameters) {
        try {
            CatalogZoneDescriptor newZone = parameters.zoneDescriptor();

            CatalogZoneDescriptor oldZone = catalogService.catalog(parameters.catalogVersion() - 1).zone(newZone.id());

            assert oldZone != null : "zoneId=" + newZone.id() + ", catalogVersion=" + parameters.catalogVersion();

            return notify(parameters, newZone, oldZone).thenApply(unused -> false);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    private CompletableFuture<Void> notify(
            AlterZoneEventParameters parameters,
            CatalogZoneDescriptor newZone,
            CatalogZoneDescriptor oldZone
    ) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        addFuture(futures, onZoneUpdate(parameters, oldZone));

        if (newZone.partitions() != oldZone.partitions()) {
            addFuture(futures, onPartitionsUpdate(parameters, oldZone.partitions()));
        }

        if (newZone.replicas() != oldZone.replicas()) {
            addFuture(futures, onReplicasUpdate(parameters, oldZone.replicas()));
        }

        if (!newZone.filter().equals(oldZone.filter())) {
            addFuture(futures, onFilterUpdate(parameters, oldZone.filter()));
        }

        if (newZone.dataNodesAutoAdjustScaleUp() != oldZone.dataNodesAutoAdjustScaleUp()) {
            addFuture(futures, onAutoAdjustScaleUpUpdate(parameters, oldZone.dataNodesAutoAdjustScaleUp()));
        }

        if (newZone.dataNodesAutoAdjustScaleDown() != oldZone.dataNodesAutoAdjustScaleDown()) {
            addFuture(futures, onAutoAdjustScaleDownUpdate(parameters, oldZone.dataNodesAutoAdjustScaleDown()));
        }

        if (!oldZone.name().equals(newZone.name())) {
            addFuture(futures, onNameUpdate(parameters, oldZone.name()));
        }

        return futures.isEmpty() ? nullCompletedFuture() : allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Called when the zone changes via {@link CatalogManager#execute(CatalogCommand)}.
     *
     * @param parameters Zone update parameters.
     * @param oldZone Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
        return nullCompletedFuture();
    }

    /**
     * Called when the zone changes via {@link CatalogManager#execute(CatalogCommand)} with a non-null
     * {@link CatalogZoneDescriptor#partitions()}.
     *
     * @param parameters Zone update parameters.
     * @param oldPartitions Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onPartitionsUpdate(AlterZoneEventParameters parameters, int oldPartitions) {
        return nullCompletedFuture();
    }

    /**
     * Called when the zone changes via {@link CatalogManager#execute(CatalogCommand)} with a non-null
     * {@link CatalogZoneDescriptor#replicas()}.
     *
     * @param parameters Zone update parameters.
     * @param oldReplicas Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
        return nullCompletedFuture();
    }

    /**
     * Called when the zone changes via {@link CatalogManager#execute(CatalogCommand)} with a non-null
     * {@link CatalogZoneDescriptor#filter()}.
     *
     * @param parameters Zone update parameters.
     * @param oldFilter Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
        return nullCompletedFuture();
    }

    /**
     * Called when a zone change via {@link CatalogManager#execute(CatalogCommand)} with a non-null
     * {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleUp()}.
     *
     * @param parameters Zone update parameters.
     * @param oldAutoAdjustScaleUp Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
        return nullCompletedFuture();
    }

    /**
     * Called when a zone change via {@link CatalogManager#execute(CatalogCommand)} with a non-null
     * {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleDown()}.
     *
     * @param parameters Zone update parameters.
     * @param oldAutoAdjustScaleDown Old value.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
        return nullCompletedFuture();
    }

    /**
     * Called when the zone name changes via {@link CatalogManager#execute(CatalogCommand)}.
     *
     * @param parameters Zone update parameters.
     * @param oldName Old zone name.
     * @return Future that signifies the end of the callback execution.
     */
    @SuppressWarnings("PMD.UnusedFormalParameter")
    protected CompletableFuture<Void> onNameUpdate(AlterZoneEventParameters parameters, String oldName) {
        return nullCompletedFuture();
    }

    private static void addFuture(List<CompletableFuture<Void>> futures, CompletableFuture<Void> future) {
        if (!future.isDone() || future.isCompletedExceptionally()) {
            futures.add(future);
        }
    }
}
