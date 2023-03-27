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

package org.apache.ignite.internal.catalog.events;

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.internal.catalog.descriptors.CatalogDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;

/**
 * Drop table event.
 */
public class DropTableEvent extends CatalogEvent {
    private static final long serialVersionUID = -4602233544705607704L;

    private final int tableId;

    public DropTableEvent(UUID opUid, int catalogVer, int tableId) {
        super(opUid, catalogVer);
        this.tableId = tableId;
    }

    public int tableId() {
        return tableId;
    }

    @Override
    public CatalogDescriptor applyTo(CatalogDescriptor catalog) {
        assert catalog != null : "Catalog of previous version must exists.";
        assert catalog.table(tableId) != null : "No table found: " + tableId;

        TableDescriptor table = catalog.table(tableId);
        SchemaDescriptor schema = catalog.schema(table.schemaName());

        assert schema != null : "No schema found: " + table.schemaName();

        return new CatalogDescriptor(
                catalogVersion(),
                System.currentTimeMillis(),
                new SchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        catalogVersion(),
                        Arrays.stream(schema.tables()).filter(t -> t.id() != tableId).toArray(TableDescriptor[]::new),
                        schema.indexes()
                )
        );

    }
}

