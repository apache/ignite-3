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

import static org.apache.ignite.internal.catalog.sql.QueryPartCollection.partsList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;

class Constraint extends QueryPart {
    private IndexType pkType;

    private final List<IndexColumnImpl> pkColumns = new ArrayList<>();

    Constraint primaryKey(ColumnSorted... columns) {
        return primaryKey(IndexType.DEFAULT, Arrays.asList(columns));
    }

    Constraint primaryKey(IndexType type, List<ColumnSorted> columns) {
        pkType = type;
        for (ColumnSorted column : columns) {
            pkColumns.add(IndexColumnImpl.wrap(column));
        }
        return this;
    }

    @Override
    protected void accept(QueryContext ctx) {
        if (!pkColumns.isEmpty()) {
            ctx.sql("PRIMARY KEY");
            if (pkType != null && pkType != IndexType.DEFAULT) {
                ctx.sql(" USING ").sql(pkType.name());
            }
            ctx.sql(" (");
            ctx.visit(partsList(pkColumns));
            ctx.sql(")");
        }
    }
}
