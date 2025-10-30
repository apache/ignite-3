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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.ensureNoTableIndexOrSysViewExistsWithGivenName;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.table;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Abstract create index command.
 *
 * <p>Encapsulates common logic of index creation like validation and update entries generation.
 */
public abstract class AbstractCreateIndexCommand extends AbstractIndexCommand {
    protected final String tableName;

    protected final boolean unique;

    protected final List<String> columns;

    private final boolean ifNotExists;

    AbstractCreateIndexCommand(
            String schemaName,
            String indexName,
            boolean ifNotExists,
            String tableName,
            boolean unique,
            List<String> columns
    ) throws CatalogValidationException {
        super(schemaName, indexName);

        validate(tableName, columns);

        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.unique = unique;
        this.columns = List.copyOf(columns);
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    protected abstract CatalogIndexDescriptor createDescriptor(int indexId, CatalogTableDescriptor table, CatalogIndexStatus status,
            boolean createdWithTable);

    @Override
    public List<UpdateEntry> get(UpdateContext context) {
        Catalog catalog = context.catalog();
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        if (ifNotExists && schema.aliveIndex(indexName) != null) {
            return List.of();
        }

        ensureNoTableIndexOrSysViewExistsWithGivenName(schema, indexName);

        CatalogTableDescriptor table = table(schema, tableName, true);

        assert columns != null;

        for (String columnName : columns) {
            if (table.column(columnName) == null) {
                throw new CatalogValidationException("Column with name '{}' not found in table '{}.{}'.",
                        columnName, schemaName, tableName);
            }
        }

        if (unique && !new HashSet<>(columns).containsAll(table.colocationColumnNames())) {
            throw new CatalogValidationException("Unique index must include all colocation columns.");
        }

        boolean indexCreatedWithTable = context.baseCatalog().table(table.id()) == null;

        CatalogIndexStatus status = indexCreatedWithTable
                ? CatalogIndexStatus.AVAILABLE
                : CatalogIndexStatus.REGISTERED;

        return List.of(
                new NewIndexEntry(createDescriptor(catalog.objectIdGenState(), table, status, indexCreatedWithTable)),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    private static void validate(String tableName, List<String> columns) {
        validateIdentifier(tableName, "Name of the table");

        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("Columns not specified.");
        }

        Set<String> columnNames = new HashSet<>();

        for (String name : columns) {
            validateIdentifier(name, "Name of the column");

            if (!columnNames.add(name)) {
                throw new CatalogValidationException("Column with name '{}' specified more than once.", name);
            }
        }
    }
}
