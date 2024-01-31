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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.tostring.S;

/**
 * A catalog snapshot entry.
 */
public class SnapshotEntry implements UpdateLogEvent {
    private static final long serialVersionUID = 3676869450184989214L;

    private final int version;
    private final long activationTime;
    private final int objectIdGenState;
    private final CatalogZoneDescriptor[] zones;
    private final CatalogSchemaDescriptor[] schemas;

    /**
     * Constructs the object.
     *
     * @param catalog Catalog instance.
     */
    public SnapshotEntry(Catalog catalog) {
        version = catalog.version();
        activationTime = catalog.time();
        objectIdGenState = catalog.objectIdGenState();
        zones = catalog.zones().toArray(CatalogZoneDescriptor[]::new);
        schemas = catalog.schemas().toArray(CatalogSchemaDescriptor[]::new);
    }

    /**
     * Returns catalog snapshot version.
     */
    public int version() {
        return version;
    }

    /**
     * Returns catalog snapshot.
     */
    public Catalog snapshot() {
        return new Catalog(
                version,
                activationTime,
                objectIdGenState,
                List.of(zones),
                List.of(schemas)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotEntry that = (SnapshotEntry) o;
        return version == that.version && activationTime == that.activationTime && objectIdGenState == that.objectIdGenState
                && Arrays.equals(zones, that.zones) && Arrays.equals(schemas, that.schemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
