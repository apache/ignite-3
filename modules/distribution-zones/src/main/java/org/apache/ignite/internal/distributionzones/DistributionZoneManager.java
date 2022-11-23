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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
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
     *
     * @param distributionZoneConfigurationParameters Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> createZone(DistributionZoneConfigurationParameters distributionZoneConfigurationParameters) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> {
                Objects.requireNonNull(distributionZoneConfigurationParameters, "Distribution zone configuration is null.");

                zonesChange.changeDistributionZones(zonesListChange ->
                        zonesListChange.create(distributionZoneConfigurationParameters.name(), zoneChange -> {
                            if (distributionZoneConfigurationParameters.dataNodesAutoAdjust() == null) {
                                zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                            } else {
                                zoneChange.changeDataNodesAutoAdjust(distributionZoneConfigurationParameters.dataNodesAutoAdjust());
                            }

                            if (distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleUp() == null) {
                                zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                            } else {
                                zoneChange.changeDataNodesAutoAdjustScaleUp(
                                        distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleUp());
                            }

                            if (distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleDown() == null) {
                                zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                            } else {
                                zoneChange
                                        .changeDataNodesAutoAdjustScaleDown(
                                                distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleDown());
                            }

                            int intZoneId = zonesChange.globalIdCounter() + 1;
                            zonesChange.changeGlobalIdCounter(intZoneId);

                            zoneChange.changeZoneId(intZoneId);
                        }));
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Alters a distribution zone.
     *
     * @param name Distribution zone name.
     * @param distributionZoneConfigurationParameters Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneConfigurationParameters distributionZoneConfigurationParameters) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> {
                Objects.requireNonNull(name, "Distribution zone name is null.");
                Objects.requireNonNull(distributionZoneConfigurationParameters, "Distribution zone configuration is null.");

                zonesChange.changeDistributionZones(zonesListChange -> zonesListChange
                        .rename(name, distributionZoneConfigurationParameters.name())
                        .update(
                                distributionZoneConfigurationParameters.name(), zoneChange -> {
                                    if (distributionZoneConfigurationParameters.dataNodesAutoAdjust() != null) {
                                        zoneChange.changeDataNodesAutoAdjust(distributionZoneConfigurationParameters.dataNodesAutoAdjust());
                                        zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                                        zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                                    }

                                    if (distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleUp() != null) {
                                        zoneChange.changeDataNodesAutoAdjustScaleUp(
                                                distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleUp());
                                        zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                    }

                                    if (distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleDown() != null) {
                                        zoneChange.changeDataNodesAutoAdjustScaleDown(
                                                distributionZoneConfigurationParameters.dataNodesAutoAdjustScaleDown());
                                        zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                    }
                                }));
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops a distribution zone with the name specified.
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
                Objects.requireNonNull(name, "Distribution zone name is null.");

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
