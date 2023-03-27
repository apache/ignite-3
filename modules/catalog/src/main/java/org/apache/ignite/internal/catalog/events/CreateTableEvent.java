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

import java.util.UUID;
import org.apache.ignite.internal.catalog.descriptors.CatalogDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * Create table event.
 */
public class CreateTableEvent extends CatalogEvent {
    private static final long serialVersionUID = -4602233544705607704L;

    private final TableDescriptor tableDescriptor;

    public CreateTableEvent(UUID opUid, int catalogVer, TableDescriptor tableDescriptor) {
        super(opUid, catalogVer);
        this.tableDescriptor = tableDescriptor;
    }

    public TableDescriptor tableDescriptor() {
        return tableDescriptor;
    }

    @Override
    public CatalogDescriptor applyTo(CatalogDescriptor catalog) {
        assert catalog != null : "Catalog of previous version must exists.";
        assert catalog.schema(tableDescriptor.schemaName()) != null : "Schema doesn't exists.";
        assert catalog.table(tableDescriptor.id()) == null : "Duplicate table.";

        SchemaDescriptor schema = catalog.schema(tableDescriptor.schemaName());

        return new CatalogDescriptor(
                catalogVersion(),
                System.currentTimeMillis(),
                new SchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        catalogVersion(),
                        ArrayUtils.concat(schema.tables(), tableDescriptor),
                        schema.indexes()
                )
        );
    }
}
