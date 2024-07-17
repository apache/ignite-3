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

import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateIdentifier;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.schemaOrThrow;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.tableOrThrow;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * A command that changes a column in a particular table.
 */
public class AlterTableAlterColumnCommand extends AbstractTableCommand {
    /** Returns command that changes a column in a particular table. */
    public static AlterTableAlterColumnCommandBuilder builder() {
        return new Builder();
    }

    private static final TypeChangeValidationListener TYPE_CHANGE_VALIDATION_HANDLER = (pattern, originalType, newType) -> {
        throw new CatalogValidationException(format(pattern, originalType, newType));
    };

    private final String columnName;

    private final @Nullable ColumnType type;

    private final @Nullable Integer precision;

    private final @Nullable Integer length;

    private final @Nullable Integer scale;

    private final @Nullable Boolean nullable;

    private final @Nullable DeferredDefaultValue deferredDefault;

    private AlterTableAlterColumnCommand(
            String tableName,
            String schemaName,
            boolean ifTableExists,
            String columnName,
            @Nullable ColumnType type,
            @Nullable Integer precision,
            @Nullable Integer length,
            @Nullable Integer scale,
            @Nullable Boolean nullable,
            @Nullable DeferredDefaultValue deferredDefault
    ) {
        super(schemaName, tableName, ifTableExists);

        this.columnName = columnName;
        this.type = type;
        this.precision = precision;
        this.length = length;
        this.scale = scale;
        this.nullable = nullable;
        this.deferredDefault = deferredDefault;

        validate();
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogSchemaDescriptor schema = schemaOrThrow(catalog, schemaName);

        CatalogTableDescriptor table = tableOrThrow(schema, tableName);

        CatalogTableColumnDescriptor origin = table.column(columnName);

        if (origin == null) {
            throw new CatalogValidationException(format(
                    "Column with name '{}' not found in table '{}.{}'", columnName, schemaName, tableName));
        }

        if (table.isPrimaryKeyColumn(origin.name())) {
            validatePkColumnChange(origin);
        } else {
            validateValueColumnChange(origin);
        }

        validateColumnChange(origin);

        CatalogTableColumnDescriptor target = createNewTableColumn(origin);

        if (origin.equals(target)) {
            // No modifications required.
            return List.of();
        }

        return List.of(
                new AlterColumnEntry(table.id(), target)
        );
    }

    private void validate() {
        validateIdentifier(columnName, "Name of the column");
    }

    private CatalogTableColumnDescriptor createNewTableColumn(CatalogTableColumnDescriptor origin) {
        return new CatalogTableColumnDescriptor(
                origin.name(),
                Objects.requireNonNullElse(type, origin.type()),
                Objects.requireNonNullElse(nullable, origin.nullable()),
                Objects.requireNonNullElse(precision, origin.precision()),
                Objects.requireNonNullElse(scale, origin.scale()),
                Objects.requireNonNullElse(length, origin.length()),
                deferredDefault != null ? deferredDefault.derive(origin.type()) : origin.defaultValue()
        );
    }

    private void validatePkColumnChange(CatalogTableColumnDescriptor origin) {
        if (type != null && type != origin.type()) {
            throw new CatalogValidationException("Changing the type of key column is not allowed");
        }
        if (precision != null && precision != origin.precision()) {
            throw new CatalogValidationException("Changing the precision of key column is not allowed");
        }
        if (scale != null && scale != origin.scale()) {
            throw new CatalogValidationException("Changing the scale of key column is not allowed");
        }
        if (nullable != null && nullable) {
            throw new CatalogValidationException("Dropping NOT NULL constraint on key column is not allowed");
        }
        if (deferredDefault != null) {
            DefaultValue defaultValue = deferredDefault.derive(origin.type());

            CatalogUtils.ensureSupportedDefault(columnName, origin.type(), defaultValue);
        }
    }

    private void validateValueColumnChange(CatalogTableColumnDescriptor origin) {
        if (deferredDefault != null) {
            DefaultValue defaultValue = deferredDefault.derive(origin.type());

            CatalogUtils.ensureNonFunctionalDefault(columnName, defaultValue);
        }
    }

    private void validateColumnChange(CatalogTableColumnDescriptor origin) {
        CatalogUtils.validateColumnChange(origin, type, precision, scale, length, TYPE_CHANGE_VALIDATION_HANDLER);

        if (nullable != null && !nullable && origin.nullable()) {
            throw new CatalogValidationException("Adding NOT NULL constraint is not allowed");
        }
    }

    private static class Builder implements AlterTableAlterColumnCommandBuilder {
        private String tableName;
        private String schemaName;
        private boolean ifTableExists;
        private String columnName;
        private @Nullable ColumnType type;
        private @Nullable Integer precision;
        private @Nullable Integer length;
        private @Nullable Integer scale;
        private @Nullable Boolean nullable;
        private @Nullable DeferredDefaultValue deferredDefault;

        @Override
        public Builder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        @Override
        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;

            return this;
        }

        @Override
        public AlterTableAlterColumnCommandBuilder ifTableExists(boolean ifTableExists) {
            this.ifTableExists = ifTableExists;

            return this;
        }

        @Override
        public Builder columnName(String columnName) {
            this.columnName = columnName;

            return this;
        }

        @Override
        public Builder type(ColumnType type) {
            this.type = type;

            return this;
        }

        @Override
        public Builder precision(int precision) {
            this.precision = precision;

            return this;
        }

        @Override
        public Builder length(int length) {
            this.length = length;

            return this;
        }

        @Override
        public Builder scale(int scale) {
            this.scale = scale;

            return this;
        }

        @Override
        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        @Override
        public Builder deferredDefaultValue(DeferredDefaultValue deferredDefault) {
            this.deferredDefault = deferredDefault;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterTableAlterColumnCommand(
                    tableName,
                    schemaName,
                    ifTableExists,
                    columnName,
                    type,
                    precision,
                    length,
                    scale,
                    nullable,
                    deferredDefault
            );
        }
    }
}
