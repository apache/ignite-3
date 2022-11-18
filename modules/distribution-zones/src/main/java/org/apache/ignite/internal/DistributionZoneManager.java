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

package org.apache.ignite.internal;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.internal.DistributionZoneView;
import org.apache.ignite.internal.DistributionZonesConfiguration;
import org.apache.ignite.internal.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** Distribution zone configuration. */
    private final DistributionZonesConfiguration zonesConfiguration;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     */
    public DistributionZoneManager(DistributionZonesConfiguration zonesConfiguration) {
        this.zonesConfiguration = zonesConfiguration;
    }

    /**
     * Creates a new distribution zone with the given {@code name} asynchronously.
     * If a distribution zone with the same name already exists,
     * a future will be completed with {@link DistributionZoneAlreadyExistsException}.
     *
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> createZone(DistributionZoneCfg distributionZoneCfg) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return CompletableFuture.supplyAsync(() -> zonesConfiguration.distributionZones().get(distributionZoneCfg.name()))
                    .thenCompose(zoneCfg -> {
                        if (zoneCfg != null) {
                            return CompletableFuture.failedFuture(new DistributionZoneAlreadyExistsException(distributionZoneCfg.name()));
                        } else {
                            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                                zonesListChange.create(distributionZoneCfg.name(), zoneChange -> {
                                    zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                                    zoneChange.changeDataNodesAutoAdjustScaleUp(distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                                    zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());

                                    int intZoneId = zonesChange.globalIdCounter() + 1;
                                    zonesChange.changeGlobalIdCounter(intZoneId);

                                    zoneChange.changeZoneId(intZoneId);
                                });
                            }));
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Alters a distribution zone. If an appropriate distribution zone does not exist,
     * a future will be completed with {@link DistributionZoneNotFoundException}.
     *
     * @param name Distribution zone name.
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneCfg distributionZoneCfg) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return CompletableFuture.supplyAsync(() -> zonesConfiguration.distributionZones().get(name))
                    .thenCompose(zoneCfg -> {
                        if (zoneCfg == null) {
                            return CompletableFuture.failedFuture(new DistributionZoneNotFoundException(name));
                        } else {
                            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                                NamedListChange<DistributionZoneView, DistributionZoneChange> change;

                                String nameForUpdate;
                                if (!name.equals(distributionZoneCfg.name())) {
                                    nameForUpdate = distributionZoneCfg.name();

                                    change = zonesListChange.rename(name, distributionZoneCfg.name());
                                } else {
                                    nameForUpdate = name;

                                    change = zonesListChange;
                                }

                                change.update(nameForUpdate, zoneChange -> {
                                    zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                                    zoneChange.changeDataNodesAutoAdjustScaleUp(distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                                    zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                                });
                            }));
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops a distribution zone with the name specified. If appropriate distribution zone does not be found, a future will be
     * completed with {@link DistributionZoneNotFoundException}.
     *
     * @param name Distribution zone name.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> dropZone(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                DistributionZoneView view = zonesListChange.get(name);

                if (view == null) {
                    throw new DistributionZoneNotFoundException(name);
                }

                zonesListChange.delete(name);
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {

    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {

    }
}
