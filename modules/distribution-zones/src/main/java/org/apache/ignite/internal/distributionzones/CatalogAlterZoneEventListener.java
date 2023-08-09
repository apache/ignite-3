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

package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.manager.EventListener;
import org.jetbrains.annotations.Nullable;

/**
 * {@link CatalogEvent#ZONE_ALTER} listener.
 *
 * <p>They will allow to listen to the change of the zone itself, as well as its fields, for this you need to override the methods:</p>
 * <ul>
 *     <li>{@link #onZoneUpdate(AlterZoneEventParameters, CatalogZoneDescriptor)};</li>
 *     <li>{@link #onPartitionsUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onReplicasUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onDataNodesAutoAdjustUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onDataNodesAutoAdjustScaleUpUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onDataNodesAutoAdjustScaleDownUpdate(AlterZoneEventParameters, int)};</li>
 *     <li>{@link #onFilterUpdate(AlterZoneEventParameters, String)}.</li>
 * </ul>
 */
// TODO: IGNITE-20114 протестировать
public class CatalogAlterZoneEventListener implements EventListener<AlterZoneEventParameters> {
    private final CatalogService catalogService;

    /** Constructor. */
    public CatalogAlterZoneEventListener(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public CompletableFuture<Boolean> notify(AlterZoneEventParameters parameters, @Nullable Throwable exception) {
        assert exception == null : parameters;

        CatalogZoneDescriptor newZone = parameters.zoneDescriptor();

        CatalogZoneDescriptor oldZone = catalogService.zone(newZone.id(), parameters.catalogVersion() - 1);

        assert oldZone != null : "zoneId=" + newZone.id() + ", catalogVersion=" + parameters.catalogVersion();

        return onUpdate(parameters, newZone, oldZone).thenApply(unused -> false);
    }

    private CompletableFuture<Void> onUpdate(
            AlterZoneEventParameters parameters,
            CatalogZoneDescriptor newZone,
            CatalogZoneDescriptor oldZone
    ) {
        List<CompletableFuture<Void>> onUpdateFutures = new ArrayList<>();

        onUpdateFutures.add(onZoneUpdate(parameters, oldZone));

        if (newZone.partitions() != oldZone.partitions()) {
            onUpdateFutures.add(onPartitionsUpdate(parameters, oldZone.partitions()));
        }

        if (newZone.replicas() != oldZone.replicas()) {
            onUpdateFutures.add(onReplicasUpdate(parameters, oldZone.replicas()));
        }

        if (!newZone.filter().equals(oldZone.filter())) {
            onUpdateFutures.add(onFilterUpdate(parameters, oldZone.filter()));
        }

        if (newZone.dataNodesAutoAdjust() != oldZone.dataNodesAutoAdjust()) {
            onUpdateFutures.add(onDataNodesAutoAdjustUpdate(parameters, oldZone.dataNodesAutoAdjust()));
        }

        if (newZone.dataNodesAutoAdjustScaleUp() != oldZone.dataNodesAutoAdjustScaleUp()) {
            onUpdateFutures.add(onDataNodesAutoAdjustScaleUpUpdate(parameters, oldZone.dataNodesAutoAdjustScaleUp()));
        }

        if (newZone.dataNodesAutoAdjustScaleDown() != oldZone.dataNodesAutoAdjustScaleDown()) {
            onUpdateFutures.add(onDataNodesAutoAdjustScaleDownUpdate(parameters, oldZone.dataNodesAutoAdjustScaleDown()));
        }

        return allOf(onUpdateFutures.toArray(CompletableFuture[]::new));
    }

    /**
     * Called when {@link CatalogZoneDescriptor} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldZone Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onZoneUpdate(AlterZoneEventParameters parameters, CatalogZoneDescriptor oldZone) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#partitions()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldPartitions Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onPartitionsUpdate(AlterZoneEventParameters parameters, int oldPartitions) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#replicas()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldReplicas Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#filter()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldFilter Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#dataNodesAutoAdjust()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldDataNodesAutoAdjust Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onDataNodesAutoAdjustUpdate(AlterZoneEventParameters parameters, int oldDataNodesAutoAdjust) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleUp()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldDataNodesAutoAdjustScaleUp Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onDataNodesAutoAdjustScaleUpUpdate(
            AlterZoneEventParameters parameters,
            int oldDataNodesAutoAdjustScaleUp
    ) {
        return completedFuture(null);
    }

    /**
     * Called when {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleDown()} changes.
     *
     * @param parameters Zone update parameters.
     * @param oldDataNodesAutoAdjustScaleDown Old value.
     * @return Future that signifies the end of the callback execution.
     */
    protected CompletableFuture<Void> onDataNodesAutoAdjustScaleDownUpdate(
            AlterZoneEventParameters parameters,
            int oldDataNodesAutoAdjustScaleDown
    ) {
        return completedFuture(null);
    }
}
