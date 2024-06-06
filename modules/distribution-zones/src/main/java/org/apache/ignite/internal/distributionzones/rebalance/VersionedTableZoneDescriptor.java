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

package org.apache.ignite.internal.distributionzones.rebalance;

import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;

/**
 *  Wrapper for {@link CatalogTableDescriptor} and {@link CatalogZoneDescriptor} matching a particular {@code catalogVersion}.
 */
public class VersionedTableZoneDescriptor {
    private final CatalogZoneDescriptor zoneDescriptor;

    private final CatalogTableDescriptor tableDescriptor;

    private final int catalogVersion;

    /**
     * Ctor.
     *
     * @param zoneDescriptor Zone descriptor.
     * @param tableDescriptor Table descriptor.
     * @param catalogVersion Catalog version.
     */
    public VersionedTableZoneDescriptor(CatalogZoneDescriptor zoneDescriptor, CatalogTableDescriptor tableDescriptor, int catalogVersion) {
        this.zoneDescriptor = zoneDescriptor;
        this.tableDescriptor = tableDescriptor;
        this.catalogVersion = catalogVersion;
    }

    public CatalogZoneDescriptor zoneDescriptor() {
        return zoneDescriptor;
    }

    public CatalogTableDescriptor tableDescriptor() {
        return tableDescriptor;
    }

    public int catalogVersion() {
        return catalogVersion;
    }

}
