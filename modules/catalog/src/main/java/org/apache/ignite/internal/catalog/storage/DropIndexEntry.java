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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of an index.
 */
public class DropIndexEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = -604729846502020728L;

    private final int indexId;

    /**
     * Constructs the object.
     *
     * @param indexId An id of an index to drop.
     */
    public DropIndexEntry(int indexId) {
        this.indexId = indexId;
    }

    /** Returns an id of an index to drop. */
    public int indexId() {
        return indexId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_DROP;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken) {
        return new DropIndexEventParameters(causalityToken, indexId);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(DEFAULT_SCHEMA_NAME));

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                List.of(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        Arrays.stream(schema.indexes()).filter(t -> t.id() != indexId).toArray(CatalogIndexDescriptor[]::new)
                ))
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
