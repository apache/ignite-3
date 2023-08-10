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

package org.apache.ignite.internal.catalog.utils;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.manager.EventListener;
import org.jetbrains.annotations.Nullable;

/**
 * Event listener for changing the distribution zone and its fields via {@link CatalogManager#alterZone(AlterZoneParams)}.
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
    public CompletableFuture<Boolean> notify(AlterZoneEventParameters parameters, @Nullable Throwable exception) {
        if (exception != null) {
            return failedFuture(exception);
        }

        try {
            CatalogZoneDescriptor newZone = parameters.zoneDescriptor();

            CatalogZoneDescriptor oldZone = catalogService.zone(newZone.id(), parameters.catalogVersion() - 1);

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

        if (newZone.dataNodesAutoAdjust() != oldZone.dataNodesAutoAdjust()) {
            addFuture(futures, onAutoAdjustUpdate(parameters, oldZone.dataNodesAutoAdjust()));
        }

        if (newZone.dataNodesAutoAdjustScaleUp() != oldZone.dataNodesAutoAdjustScaleUp()) {
            addFuture(futures, onAutoAdjustScaleUpUpdate(parameters, oldZone.dataNodesAutoAdjustScaleUp()));
        }

        if (newZone.dataNodesAutoAdjustScaleDown() != oldZone.dataNodesAutoAdjustScaleDown()) {
            addFuture(futures, onAutoAdjustScaleDownUpdate(parameters, oldZone.dataNodesAutoAdjustScaleDown()));
        }

        return futures.isEmpty() ? completedFuture(null) : allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)}.
     *
     * @param parameters Zone update parameters.
     * @param oldZone Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null {@link AlterZoneParams#partitions()}.
     *
     * @param parameters Zone update parameters.
     * @param oldPartitions Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onPartitionsUpdate(AlterZoneEventParameters parameters, int oldPartitions) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null {@link AlterZoneParams#replicas()}.
     *
     * @param parameters Zone update parameters.
     * @param oldReplicas Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null {@link AlterZoneParams#filter()}.
     *
     * @param parameters Zone update parameters.
     * @param oldFilter Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null
     * {@link AlterZoneParams#dataNodesAutoAdjust()}.
     *
     * @param parameters Zone update parameters.
     * @param oldAutoAdjust Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onAutoAdjustUpdate(AlterZoneEventParameters parameters, int oldAutoAdjust) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null
     * {@link AlterZoneParams#dataNodesAutoAdjustScaleUp()}.
     *
     * @param parameters Zone update parameters.
     * @param oldAutoAdjustScaleUp Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
        return completedFuture(null);
    }

    /**
     * Called when a zone change via {@link CatalogManager#alterZone(AlterZoneParams)} with a non-null
     * {@link AlterZoneParams#dataNodesAutoAdjustScaleDown()}.
     *
     * @param parameters Zone update parameters.
     * @param oldAutoAdjustScaleDown Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
        return completedFuture(null);
    }

    private static void addFuture(List<CompletableFuture<Void>> futures, CompletableFuture<Void> future) {
        if (!future.isDone() || future.isCompletedExceptionally()) {
            futures.add(future);
        }
    }
}
