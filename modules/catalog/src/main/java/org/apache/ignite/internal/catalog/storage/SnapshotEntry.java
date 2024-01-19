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

package org.apache.ignite.internal.catalog.storage;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;

/**
 * Describes catalog snapshot.
 */
public class SnapshotEntry implements UpdateEntry {
    private static final long serialVersionUID = 5363360159820865687L;

    private final int version;
    private final long activationTime;
    private final int objectIdGenState;
    private final CatalogZoneDescriptor[] zones;
    private final CatalogSchemaDescriptor[] schemas;

    public SnapshotEntry(Catalog catalog) {
        version = catalog.version();
        activationTime = catalog.time();
        objectIdGenState = catalog.objectIdGenState();
        zones = catalog.zones().toArray(CatalogZoneDescriptor[]::new);
        schemas = catalog.schemas().toArray(CatalogSchemaDescriptor[]::new);
    }

    public long time() {
        return activationTime;
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        assert catalog.version() < version;

        return new Catalog(
                version,
                activationTime,
                objectIdGenState,
                List.of(zones),
                List.of(schemas)
        );
    }
}
