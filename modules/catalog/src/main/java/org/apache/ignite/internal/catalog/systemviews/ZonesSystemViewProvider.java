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
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogSystemViewProvider;
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
        SystemView<?> zoneSystemView = SystemViews.<ZoneWithDefaultMarker>clusterViewBuilder()
                .name("ZONES")
                .addColumn("NAME", STRING, wrapper -> wrapper.zone.name())
                .addColumn("PARTITIONS", INT32, wrapper -> wrapper.zone.partitions())
                .addColumn("REPLICAS", INT32, wrapper -> wrapper.zone.replicas())
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_UP", INT32, wrapper -> wrapper.zone.dataNodesAutoAdjustScaleUp())
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_DOWN", INT32, wrapper -> wrapper.zone.dataNodesAutoAdjustScaleDown())
                .addColumn("DATA_NODES_FILTER", STRING, wrapper -> wrapper.zone.filter())
                .addColumn("IS_DEFAULT_ZONE", BOOLEAN, wrapper -> wrapper.isDefault)
                .dataProvider(SubscriptionUtils.fromIterable(() -> {
                            Catalog catalog = catalogSupplier.get();
                            CatalogZoneDescriptor defaultZone = catalog.defaultZone();
                            return new TransformingIterator<>(catalog.zones().iterator(),
                                    (zone) -> new ZoneWithDefaultMarker(zone, defaultZone != null && defaultZone.id() == zone.id()));
                        }
                ))
                .build();

        return List.of(zoneSystemView);
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
}
