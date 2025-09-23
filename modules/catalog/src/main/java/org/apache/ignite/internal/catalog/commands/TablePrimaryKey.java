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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.jetbrains.annotations.Nullable;

/** Base class for a primary key. */
public abstract class TablePrimaryKey {

    private final @Nullable String name;
    private final List<String> columns;

    /**
     * Constructor.
     *
     * @param columns List of columns.
     */
    TablePrimaryKey(@Nullable String name, List<String> columns) {
        this.name = name;
        this.columns = columns != null ? List.copyOf(columns) : List.of();
    }

    /** Returns a list name of columns that comprise this primary key. */
    public List<String> columns() {
        return columns;
    }

    public @Nullable String name() {
        return name;
    }

    /** Performs additional validation of this primary key. */
    void validate(List<ColumnParams> allColumns) {
        if (name != null) {
            validateIdentifier(name, "Name of the primary key constraint");
        }

        Set<String> allColumnNames = new HashSet<>(allColumns.size());
        for (ColumnParams column : allColumns) {
            allColumnNames.add(column.name());

            boolean partOfPk = columns.contains(column.name());
            if (partOfPk && column.nullable()) {
                throw new CatalogValidationException("Primary key cannot contain nullable column [col={}].", column.name());
            }
        }

        List<String> columnsNotInTable = columns.stream()
                .filter(Predicate.not(allColumnNames::contains))
                .collect(Collectors.toList());
        if (!columnsNotInTable.isEmpty()) {
            throw new CatalogValidationException("Primary key constraint contains undefined columns: [cols={}].", columnsNotInTable);
        }

        Set<String> columnSet = new HashSet<>();
        for (String name : columns) {
            if (!columnSet.add(name)) {
                throw new CatalogValidationException("PK column '{}' specified more that once.", name);
            }
        }
    }

    /** Base class for a builder of a primary key. */
    public abstract static class TablePrimaryKeyBuilder<T extends TablePrimaryKeyBuilder<T>> {

        /** Specifies a list of primary key columns. */
        public abstract T columns(List<String> columns);

        /** Specifies a name of primary key. May be null. */
        public abstract T name(@Nullable String name);

        /** Creates primary key. */
        public abstract TablePrimaryKey build();
    }
}
