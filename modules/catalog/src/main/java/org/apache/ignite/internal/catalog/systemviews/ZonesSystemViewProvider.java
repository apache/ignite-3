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

package org.apache.ignite.internal.catalog.systemviews;

import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.TransformingIterator;

/**
 * Exposes information on zones.
 *
 * <ul>
 *     <li>ZONES system view</li>
 * </ul>
 */
public final class ZonesSystemViewProvider implements CatalogSystemViewProvider {

    /** {@inheritDoc} */
    @Override
    public List<SystemView<?>> getView(Supplier<Catalog> catalogSupplier) {
        return List.of(getZoneView(catalogSupplier), getStorageProfilesView(catalogSupplier));
    }

    private static SystemView<?> getZoneView(Supplier<Catalog> catalogSupplier) {
        return SystemViews.<ZoneWithDefaultMarker>clusterViewBuilder()
                .name("ZONES")
                .addColumn("ZONE_NAME", STRING, wrapper -> wrapper.zone.name())
                .addColumn("ZONE_PARTITIONS", INT32, wrapper -> wrapper.zone.partitions())
                .addColumn("ZONE_REPLICAS", INT32, wrapper -> wrapper.zone.replicas())
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_UP", INT32, wrapper -> wrapper.zone.dataNodesAutoAdjustScaleUp())
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_DOWN", INT32, wrapper -> wrapper.zone.dataNodesAutoAdjustScaleDown())
                .addColumn("DATA_NODES_FILTER", STRING, wrapper -> wrapper.zone.filter())
                .addColumn("IS_DEFAULT_ZONE", BOOLEAN, wrapper -> wrapper.isDefault)
                .addColumn("ZONE_CONSISTENCY_MODE", STRING, wrapper -> wrapper.zone.consistencyMode().name())
                .addColumn("ZONE_ID", INT32, wrapper -> wrapper.zone.id())
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .addColumn("NAME", STRING, wrapper -> wrapper.zone.name())
                .addColumn("PARTITIONS", INT32, wrapper -> wrapper.zone.partitions())
                .addColumn("REPLICAS", INT32, wrapper -> wrapper.zone.replicas())
                .addColumn("CONSISTENCY_MODE", STRING, wrapper -> wrapper.zone.consistencyMode().name())
                // End of legacy columns list. New columns must be added below this line.
                .addColumn("ZONE_QUORUM_SIZE", INT32, wrapper -> wrapper.zone.quorumSize())
                .dataProvider(SubscriptionUtils.fromIterable(() -> {
                            Catalog catalog = catalogSupplier.get();
                            CatalogZoneDescriptor defaultZone = catalog.defaultZone();
                            return new TransformingIterator<>(catalog.zones().iterator(),
                                    (zone) -> new ZoneWithDefaultMarker(zone, defaultZone != null && defaultZone.id() == zone.id()));
                        }
                ))
                .build();
    }

    private static SystemView<?> getStorageProfilesView(Supplier<Catalog> catalogSupplier) {
        Iterable<ZoneWithProfile> viewData = () -> {
            Catalog catalog = catalogSupplier.get();

            return catalog.zones().stream()
                    .flatMap(zone -> {
                        List<CatalogStorageProfileDescriptor> profiles = zone.storageProfiles().profiles();
                        CatalogStorageProfileDescriptor defaultProfile = zone.storageProfiles().defaultProfile();

                        return profiles.stream().map(profile ->
                                new ZoneWithProfile(
                                        zone,
                                        profile.storageProfile(),
                                        Objects.equals(profile.storageProfile(), defaultProfile.storageProfile())
                                )
                        );
                    }).iterator();
        };

        return SystemViews.<ZoneWithProfile>clusterViewBuilder()
                .name("ZONE_STORAGE_PROFILES")
                .addColumn("ZONE_NAME", STRING, zone -> zone.descriptor.name())
                .addColumn("STORAGE_PROFILE", STRING, zone -> zone.profileName)
                .addColumn("IS_DEFAULT_PROFILE", BOOLEAN, zone -> zone.isDefaultProfile)
                .addColumn("ZONE_ID", INT32, zone -> zone.descriptor.id())
                .dataProvider(SubscriptionUtils.fromIterable(viewData))
                .build();
    }

    /** Wraps a CatalogZoneDescriptor and a flag indicating whether this zone is the default zone. */
    static class ZoneWithDefaultMarker {
        private final CatalogZoneDescriptor zone;
        private final boolean isDefault;

        ZoneWithDefaultMarker(CatalogZoneDescriptor zone, boolean isDefault) {
            this.zone = zone;
            this.isDefault = isDefault;
        }
    }

    /**
     * Wraps a zone and a one of storage profile of the zone.
     */
    private static class ZoneWithProfile {
        private final CatalogZoneDescriptor descriptor;
        private final String profileName;
        private final boolean isDefaultProfile;

        private ZoneWithProfile(CatalogZoneDescriptor zone, String profileName, boolean defaultProfile) {
            this.descriptor = zone;
            this.profileName = profileName;
            this.isDefaultProfile = defaultProfile;
        }
    }
}
