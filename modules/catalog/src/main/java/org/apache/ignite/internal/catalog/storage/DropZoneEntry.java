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

import static java.util.stream.Collectors.toList;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of a zone.
 */
public class DropZoneEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = 7727583734058987315L;

    private final int zoneId;

    /**
     * Constructs the object.
     *
     * @param zoneId An id of a zone to drop.
     */
    public DropZoneEntry(int zoneId) {
        this.zoneId = zoneId;
    }

    /** Returns an id of a zone to drop. */
    public int zoneId() {
        return zoneId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.ZONE_DROP;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropZoneEventParameters(causalityToken, catalogVersion, zoneId);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones().stream().filter(z -> z.id() != zoneId).collect(toList()),
                catalog.schemas()
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
