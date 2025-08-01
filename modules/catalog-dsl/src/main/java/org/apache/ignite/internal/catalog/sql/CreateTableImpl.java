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

package org.apache.ignite.internal.catalog.sql;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.catalog.sql.QueryPartCollection.partsList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;

class CreateTableImpl extends AbstractCatalogQuery<Name> {
    private Name tableName;

    private boolean ifNotExists;

    private final List<Column> columns = new ArrayList<>();

    private final List<Constraint> constraints = new ArrayList<>();

    private Colocate colocate;

    private Zone zone;

    private final List<CreateIndexImpl> indexes = new ArrayList<>();

    /**
     * Constructor for internal usage.
     *
     * @see CreateFromAnnotationsImpl
     */
    CreateTableImpl(IgniteSql sql) {
        super(sql);
    }

    @Override
    protected Name result() {
        return tableName;
    }

    CreateTableImpl name(QualifiedName name) {
        Objects.requireNonNull(name, "Table name must not be null.");

        this.tableName = Name.qualified(name);
        return this;
    }

    CreateTableImpl ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    CreateTableImpl addColumn(String name, String definition) {
        Objects.requireNonNull(name, "Column name must not be null.");
        Objects.requireNonNull(definition, "Column type must not be null.");

        columns.add(new Column(name, definition));
        return this;
    }

    CreateTableImpl addColumn(String name, ColumnType<?> type) {
        Objects.requireNonNull(name, "Column name must not be null.");
        Objects.requireNonNull(type, "Column type must not be null.");

        columns.add(new Column(name, ColumnTypeImpl.wrap(type)));
        return this;
    }

    CreateTableImpl primaryKey(List<String> columns) {
        return primaryKey(IndexType.DEFAULT, columns.stream().map(ColumnSorted::column).collect(Collectors.toList()));
    }

    CreateTableImpl primaryKey(IndexType type, List<ColumnSorted> columns) {
        Objects.requireNonNull(columns, "PK columns must not be null.");

        constraints.add(new Constraint().primaryKey(type, columns));
        return this;
    }

    CreateTableImpl colocateBy(String... columns) {
        return colocateBy(asList(columns));
    }

    CreateTableImpl colocateBy(List<String> columns) {
        Objects.requireNonNull(columns, "Colocate columns must not be null.");

        colocate = new Colocate(columns);
        return this;
    }

    CreateTableImpl zone(String zone) {
        Objects.requireNonNull(zone, "Zone name must not be null.");

        this.zone = new Zone(zone);
        return this;
    }

    CreateTableImpl addIndex(String name, IndexType type, List<ColumnSorted> columns) {
        Objects.requireNonNull(name, "Index name must not be null.");
        Objects.requireNonNull(columns, "Index columns list must not be null.");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Index columns list must not be empty.");
        }

        if (type == IndexType.HASH) {
            for (ColumnSorted c : columns) {
                if (c.sortOrder() != SortOrder.DEFAULT) {
                    throw new IllegalArgumentException("Index columns must not define a sort order in hash indexes.");
                }
            }
        }

        indexes.add(new CreateIndexImpl(sql).ifNotExists().name(name).using(type).on(tableName, columns));
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Columns list must not be empty.");
        }

        ctx.sql("CREATE TABLE ");

        if (ifNotExists) {
            ctx.sql("IF NOT EXISTS ");
        }

        ctx.visit(tableName);

        ctx.sqlIndentStart(" (");

        ctx.visit(partsList(columns));

        if (!constraints.isEmpty()) {
            ctx.sql(", ");
            ctx.visit(partsList(constraints));
        }

        ctx.sql(")");

        if (colocate != null) {
            ctx.sql(" ").visit(colocate);
        }

        if (zone != null) {
            ctx.sql(" ").visit(zone);
        }

        ctx.sql(";");

        for (CreateIndexImpl index : indexes) {
            ctx.formatSeparator().visit(index);
        }
    }
}
