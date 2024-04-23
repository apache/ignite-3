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

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateSystemViewEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Describes addition of a new system view.
 */
public class NewSystemViewEntry implements UpdateEntry, Fireable {
    public static final CatalogObjectSerializer<NewSystemViewEntry> SERIALIZER = new NewSystemViewEntrySerializer();

    private final CatalogSystemViewDescriptor descriptor;

    private final String schemaName;

    /**
     * Constructor.
     *
     * @param descriptor System view descriptor.
     * @param schemaName A schema name.
     */
    public NewSystemViewEntry(CatalogSystemViewDescriptor descriptor, String schemaName) {
        this.descriptor = descriptor;
        this.schemaName = schemaName;
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.NEW_SYS_VIEW.id();
    }

    /** {@inheritDoc} */
    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.SYSTEM_VIEW_CREATE;
    }

    /** {@inheritDoc} */
    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new CreateSystemViewEventParameters(causalityToken, catalogVersion, descriptor);
    }

    /** {@inheritDoc} */
    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        CatalogSchemaDescriptor systemSchema = catalog.schema(schemaName);

        descriptor.updateToken(causalityToken);

        Map<String, CatalogSystemViewDescriptor> systemViews = Arrays.stream(systemSchema.systemViews())
                .collect(Collectors.toMap(CatalogSystemViewDescriptor::name, Function.identity()));
        systemViews.put(descriptor.name(), descriptor);

        CatalogSystemViewDescriptor[] sysViewArray = systemViews.values().toArray(new CatalogSystemViewDescriptor[0]);

        CatalogSchemaDescriptor newSystemSchema = new CatalogSchemaDescriptor(
                systemSchema.id(),
                systemSchema.name(),
                systemSchema.tables(),
                systemSchema.indexes(),
                sysViewArray,
                causalityToken);

        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState(),
                catalog.zones(),
                CatalogUtils.replaceSchema(newSystemSchema, catalog.schemas()),
                defaultZoneIdOpt(catalog)
        );
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link NewSystemViewEntry}.
     */
    private static class NewSystemViewEntrySerializer implements CatalogObjectSerializer<NewSystemViewEntry> {
        @Override
        public NewSystemViewEntry readFrom(IgniteDataInput input) throws IOException {
            CatalogSystemViewDescriptor descriptor = CatalogSystemViewDescriptor.SERIALIZER.readFrom(input);
            String schema = input.readUTF();

            return new NewSystemViewEntry(descriptor, schema);
        }

        @Override
        public void writeTo(NewSystemViewEntry entry, IgniteDataOutput output) throws IOException {
            CatalogSystemViewDescriptor.SERIALIZER.writeTo(entry.descriptor, output);
            output.writeUTF(entry.schemaName);
        }
    }
}
