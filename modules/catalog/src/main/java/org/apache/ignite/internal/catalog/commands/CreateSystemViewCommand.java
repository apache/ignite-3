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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.util.CollectionUtils.copyOrNull;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogParamsValidationUtils;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.UpdateContext;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.storage.NewSystemViewEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Create system view command - creates or replaces a system view.
 *
 * <p>If a system with the given name does not exist, this command adds this system view to {@code SYSTEM} schema.
 *
 * <p>If a system with the given name exists, this command replaces the existing system view.
 */
public class CreateSystemViewCommand implements CatalogCommand {

    private final String name;

    private final List<ColumnParams> columns;

    private final SystemViewType systemViewType;

    /**
     * Constructor.
     *
     * @param name View name.
     * @param columns List of view columns.
     * @param systemViewType View type.
     */
    private CreateSystemViewCommand(String name, List<ColumnParams> columns, SystemViewType systemViewType) {
        this.name = name;
        this.columns = copyOrNull(columns);
        this.systemViewType = systemViewType;

        validate();
    }

    /**
     * Returns a view name.
     *
     * @return View name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns a list of view columns.
     *
     * @return List of view columns.
     */
    public List<ColumnParams> columns() {
        return columns;
    }

    /**
     * Returns a view type.
     *
     * @return View type.
     */
    public SystemViewType systemViewType() {
        return systemViewType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        Catalog catalog = updateContext.catalog();
        int id = catalog.objectIdGenState();

        CatalogSchemaDescriptor systemSchema = schemaOrThrow(catalog, CatalogManager.SYSTEM_SCHEMA_NAME);

        List<CatalogTableColumnDescriptor> viewColumns = columns.stream().map(CatalogUtils::fromParams).collect(toList());
        CatalogSystemViewDescriptor descriptor = new CatalogSystemViewDescriptor(id, systemSchema.id(), name, viewColumns, systemViewType);

        CatalogSystemViewDescriptor existingSystemView = systemSchema.systemView(name);

        if (existingSystemView == null) {
            // If view does not exists, ensure that the given name is not used by other objects.
            CatalogParamsValidationUtils.ensureNoTableIndexOrSysViewExistsWithGivenName(systemSchema, name);
        } else if (descriptor.equals(existingSystemView)) {
            // If the same view exists, do not update the catalog.
            return List.of();
        }

        return List.of(
                new NewSystemViewEntry(descriptor),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    private void validate() {
        validateIdentifier(name, "Name of the system view");

        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("System view should have at least one column.");
        }

        if (systemViewType == null) {
            throw new CatalogValidationException("System view type is not specified.");
        }

        Set<String> columnNames = new HashSet<>();

        for (ColumnParams column : columns) {
            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException("Column with name '{}' specified more than once.", column.name());
            }
        }
    }

    /**
     * Creates a builder to construct instances of create system view command.
     *
     * @return A builder to create instances of create system view command.
     */
    public static CreateSystemViewCommandBuilder builder() {
        return new Builder();
    }

    /**
     * Implementation of {@link CreateSystemViewCommandBuilder}.
     */
    private static class Builder implements CreateSystemViewCommandBuilder {

        private String name;

        private List<ColumnParams> columns;

        private SystemViewType systemViewType;

        /**
         * Constructor.
         */
        Builder() {

        }

        /** {@inheritDoc} */
        @Override
        public CreateSystemViewCommandBuilder name(String name) {
            this.name = name;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public CreateSystemViewCommandBuilder type(SystemViewType systemViewType) {
            this.systemViewType = systemViewType;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public CreateSystemViewCommandBuilder columns(List<ColumnParams> columns) {
            this.columns = columns;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public CreateSystemViewCommand build() {
            return new CreateSystemViewCommand(name, columns, systemViewType);
        }
    }
}
