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

import static org.apache.ignite.internal.catalog.sql.IndexColumnImpl.wrap;
import static org.apache.ignite.internal.catalog.sql.QueryPartCollection.partsList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Options;
import org.apache.ignite.sql.IgniteSql;

class CreateIndexImpl extends AbstractCatalogQuery {
    private Name indexName;

    private boolean ifNotExists;

    private final List<IndexColumnImpl> columns = new ArrayList<>();

    private Name tableName;

    private IndexType indexType;

    /**
     * Constructor for internal usage.
     *
     * @see CreateFromAnnotationsImpl
     */
    CreateIndexImpl(IgniteSql sql, Options options) {
        super(sql, options);
    }

    CreateIndexImpl name(String... names) {
        Objects.requireNonNull(names, "Index name must not be null.");

        indexName = new Name(names);
        return this;
    }

    CreateIndexImpl ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    public CreateIndexImpl on(Name tableName, List<ColumnSorted> columns) {
        this.tableName = tableName;
        for (ColumnSorted column : columns) {
            this.columns.add(wrap(column));
        }
        return this;
    }

    public CreateIndexImpl using(IndexType type) {
        this.indexType = type;
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql("CREATE INDEX ");
        if (ifNotExists) {
            ctx.sql("IF NOT EXISTS ");
        }
        ctx.visit(indexName);
        ctx.sql(" ON ");
        ctx.visit(tableName);

        if (indexType != null && indexType != IndexType.DEFAULT) {
            ctx.sql(" USING ").sql(indexType.name());
        }

        ctx.sqlIndentStart(" (");
        if (!columns.isEmpty()) {
            ctx.visit(partsList(columns).formatSeparator());
        }

        ctx.sqlIndentEnd(")");

        ctx.sql(";");
    }
}
