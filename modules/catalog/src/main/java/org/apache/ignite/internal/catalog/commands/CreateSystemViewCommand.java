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
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateColumnParams;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.storage.NewSystemViewEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Create system view command - creates or replaces a system view.
 *
 * <p>If a system with with the given name does not exists, this command adds this system view to {@code SYSTEM} schema.
 * If a system with with the given name exists, this command replaces the existing system view.
 */
public class CreateSystemViewCommand extends AbstractCatalogCommand {

    private final String name;

    private final List<ColumnParams> columns;

    /**
     * Constructor.
     *
     * @param name View name.
     * @param columns List of view columns.
     */
    CreateSystemViewCommand(String name, List<ColumnParams> columns) {
        this.name = name;
        this.columns = columns;

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
     * {@inheritDoc}
     */
    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        int id = catalog.objectIdGenState();

        List<CatalogTableColumnDescriptor> viewColumns = columns.stream().map(CatalogUtils::fromParams).collect(toList());
        CatalogSystemViewDescriptor descriptor = new CatalogSystemViewDescriptor(id, name, viewColumns);

        return List.of(
                new NewSystemViewEntry(descriptor),
                new ObjectIdGenUpdateEntry(1)
        );
    }

    private void validate() {
        validateIdentifier(name, "Name of the system view");

        if (nullOrEmpty(columns)) {
            throw new CatalogValidationException("System view should have at least one column");
        }

        Set<String> columnNames = new HashSet<>();

        for (ColumnParams column : columns) {
            validateColumnParams(column);

            if (!columnNames.add(column.name())) {
                throw new CatalogValidationException(Common.INTERNAL_ERR,
                        format("Column with name '{}' specified more than once", column.name()));
            }
        }
    }

    /**
     * Creates a builder to construct instances of create system view command.
     *
     * @return A builder to create instances of create system view command.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Implementation of {@link CreateSystemViewCommandBuilder}.
     */
    public static class Builder implements CreateSystemViewCommandBuilder {

        private String name;

        private List<ColumnParams> columns;

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
        public CreateSystemViewCommandBuilder columns(List<ColumnParams> columns) {
            this.columns = columns;
            return this;
        }

        /** {@inheritDoc} */
        @Override
        public CreateSystemViewCommand build() {
            return new CreateSystemViewCommand(name, columns);
        }
    }
}
