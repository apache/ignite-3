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
import static org.apache.ignite.internal.catalog.sql.IndexColumnImpl.parseIndexColumnList;
import static org.apache.ignite.internal.catalog.sql.QueryPartCollection.partsList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.sql.IgniteSql;

class CreateTableImpl extends AbstractCatalogQuery<Name> {
    private Name tableName;

    private boolean ifNotExists;

    private final List<Column> columns = new ArrayList<>();

    private final List<Constraint> constraints = new ArrayList<>();

    private final List<WithOption> withOptions = new ArrayList<>();

    private Colocate colocate;

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

    CreateTableImpl name(String... names) {
        Objects.requireNonNull(names, "Table name must not be null.");

        this.tableName = new Name(names);
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

    CreateTableImpl primaryKey(String columnList) {
        return primaryKey(IndexType.DEFAULT, columnList);
    }

    CreateTableImpl primaryKey(IndexType type, String columnList) {
        return primaryKey(type, parseIndexColumnList(columnList));
    }

    CreateTableImpl primaryKey(IndexType type, List<ColumnSorted> columns) {
        Objects.requireNonNull(columns, "PK columns must not be null.");

        constraints.add(new Constraint().primaryKey(type, columns));
        return this;
    }

    CreateTableImpl colocateBy(String columnList) {
        return colocateBy(QueryUtils.splitByComma(columnList));
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

        withOptions.add(WithOption.primaryZone(zone));
        return this;
    }

    CreateTableImpl addIndex(String name, String columnList) {
        return addIndex(name, null, columnList);
    }

    CreateTableImpl addIndex(String name, IndexType type, String columnList) {
        return addIndex(name, type, parseIndexColumnList(columnList));
    }

    CreateTableImpl addIndex(String name, IndexType type, ColumnSorted... columns) {
        return addIndex(name, type, asList(columns));
    }

    CreateTableImpl addIndex(String name, IndexType type, List<ColumnSorted> columns) {
        Objects.requireNonNull(name, "Index name must not be null.");
        Objects.requireNonNull(columns, "Index columns list must not be null.");

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

        if (!withOptions.isEmpty()) {
            ctx.sql(" ").sql("WITH ");
            ctx.visit(partsList(withOptions));
        }

        ctx.sql(";");

        for (CreateIndexImpl index : indexes) {
            ctx.formatSeparator().visit(index);
        }
    }
}
