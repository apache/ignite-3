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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.CatalogValidationException;

/** Base class for a primary key. */
public abstract class TablePrimaryKey {

    private final List<String> columns;

    /**
     * Constructor.
     *
     * @param columns List of columns.
     */
    TablePrimaryKey(List<String> columns) {
        this.columns = columns != null ? List.copyOf(columns) : List.of();
    }

    /** Returns a list name of columns that comprise this primary key. */
    public List<String> columns() {
        return columns;
    }

    /** Performs additional validation of this primary key. */
    void validate(List<ColumnParams> allColumns) {
        Set<String> allColumnNames = new HashSet<>(allColumns.size());
        for (ColumnParams column : allColumns) {
            allColumnNames.add(column.name());

            boolean partOfPk = columns.contains(column.name());
            if (partOfPk && column.nullable()) {
                throw new CatalogValidationException(format("Primary key cannot contain nullable column [col={}].", column.name()));
            }
        }

        List<String> columnsNotInTable = columns.stream()
                .filter(Predicate.not(allColumnNames::contains))
                .collect(Collectors.toList());
        if (!columnsNotInTable.isEmpty()) {
            throw new CatalogValidationException(
                    format("Primary key constraint contains undefined columns: [cols={}].", columnsNotInTable));
        }

        Set<String> columnSet = new HashSet<>();
        for (String name : columns) {
            if (!columnSet.add(name)) {
                throw new CatalogValidationException(format("PK column '{}' specified more that once.", name));
            }
        }
    }

    /** Base class for a builder of a primary key. */
    public abstract static class TablePrimaryKeyBuilder<T extends TablePrimaryKeyBuilder<T>> {

        /** Specifies a list of primary key columns. */
        public abstract T columns(List<String> columns);

        /** Creates primary key. */
        public abstract TablePrimaryKey build();
    }
}
