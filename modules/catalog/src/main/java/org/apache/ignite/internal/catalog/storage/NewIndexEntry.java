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

import static org.apache.ignite.internal.catalog.CatalogService.PUBLIC;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Describes addition of a new index.
 */
public class NewIndexEntry implements UpdateEntry, Fireable {
    private static final long serialVersionUID = 6717363577013237711L;

    private final CatalogIndexDescriptor descriptor;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of an index to add.
     */
    public NewIndexEntry(CatalogIndexDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Gets descriptor of an index to add. */
    public CatalogIndexDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_CREATE;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken) {
        return new CreateIndexEventParameters(causalityToken, descriptor);
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, VersionedUpdate update) {
        CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(PUBLIC));

        return new Catalog(
                update.version(),
                update.activationTimestamp(),
                catalog.objectIdGenState(),
                catalog.zones(),
                List.of(new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        ArrayUtils.concat(schema.indexes(), descriptor)
                ))
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
