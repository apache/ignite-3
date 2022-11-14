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

import org.apache.ignite.internal.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;

import java.util.concurrent.CompletableFuture;

public class DistributionZoneManager implements IgniteComponent {
    DistributionZonesConfiguration zonesConfiguration;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    public DistributionZoneManager(DistributionZonesConfiguration zonesConfiguration) {
        this.zonesConfiguration = zonesConfiguration;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {

    }

    public CompletableFuture<Void> createZone(String name, int autoAdjustScaleUp, int autoAdjustScaleDown) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            DistributionZoneConfiguration cfg = zonesConfiguration.distributionZones().get(name);

            if (cfg != null) {
                return CompletableFuture.failedFuture(new DistributionZoneAlreadyExistsException(name));
            }

            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                zonesListChange.create(name, zoneChange -> {
                    zoneChange.changeDataNodesAutoAdjustScaleUp(autoAdjustScaleUp);
                    zoneChange.changeDataNodesAutoAdjustScaleDown(autoAdjustScaleDown);
                    zoneChange.changeDataNodesAutoAdjust(0);

                    int intZoneId = zonesChange.globalIdCounter() + 1;
                    zonesChange.changeGlobalIdCounter(intZoneId);
                });
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    public CompletableFuture<Void> createZone(String name, int autoAdjust) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            DistributionZoneConfiguration cfg = zonesConfiguration.distributionZones().get(name);

            if (cfg != null) {
                return CompletableFuture.failedFuture(new DistributionZoneAlreadyExistsException(name));
            }

            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                zonesListChange.create(name, zoneChange -> {
                    zoneChange.changeDataNodesAutoAdjustScaleUp(0);
                    zoneChange.changeDataNodesAutoAdjustScaleDown(0);
                    zoneChange.changeDataNodesAutoAdjust(autoAdjust);

                    int intZoneId = zonesChange.globalIdCounter() + 1;
                    zonesChange.changeGlobalIdCounter(intZoneId);
                });
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    public CompletableFuture<Void> dropZone(String name) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }
        try {
            DistributionZoneConfiguration cfg = zonesConfiguration.distributionZones().get(name);

            if (cfg == null) {
                return CompletableFuture.failedFuture(new DistributionZoneNotFoundException(name));
            }

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
}
