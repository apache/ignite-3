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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * New schema entry.
 */
public class NewSchemaEntry implements UpdateEntry {
    public static final CatalogObjectSerializer<NewSchemaEntry> SERIALIZER = new Serializer();

    private final CatalogSchemaDescriptor descriptor;

    public NewSchemaEntry(CatalogSchemaDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor schema = catalog.schema(descriptor.name());
        if (schema != null) {
            return catalog;
        }

        descriptor.updateToken(causalityToken);

        List<CatalogSchemaDescriptor> schemas = new ArrayList<>(catalog.schemas().size() + 1);
        schemas.addAll(catalog.schemas());
        schemas.add(descriptor);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                schemas,
                catalog.defaultZone().id()
        );
    }

    /** {@inheritDoc} */
    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_SCHEMA.id();
    }

    private static class Serializer implements CatalogObjectSerializer<NewSchemaEntry> {
        @Override
        public NewSchemaEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogSchemaDescriptor schemaDescriptor = CatalogSchemaDescriptor.SERIALIZER.readFrom(input);
            return new NewSchemaEntry(schemaDescriptor);
        }

        @Override
        public void writeTo(NewSchemaEntry value, IgniteDataOutput output) throws IOException {
            CatalogSchemaDescriptor.SERIALIZER.writeTo(value.descriptor, output);
        }
    }
}
