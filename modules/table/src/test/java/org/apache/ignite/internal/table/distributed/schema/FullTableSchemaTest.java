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

package org.apache.ignite.internal.table.distributed.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

class FullTableSchemaTest {
    private static final String TABLE_NAME1 = "test1";
    private static final String TABLE_NAME2 = "test2";

    private static CatalogTableColumnDescriptor someColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, ColumnType.INT32, true, 0, 0, 0, DefaultValue.constant(null));
    }

    @Test
    void addedRemovedColumnsAreReflectedInDiff() {
        CatalogTableColumnDescriptor column1 = someColumn("a");
        CatalogTableColumnDescriptor column2 = someColumn("b");
        CatalogTableColumnDescriptor column3 = someColumn("c");

        var schema1 = new FullTableSchema(1, 1, TABLE_NAME1, List.of(column1, column2));
        var schema2 = new FullTableSchema(2, 1, TABLE_NAME1, List.of(column2, column3));

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.addedColumns(), is(List.of(column3)));
        assertThat(diff.removedColumns(), is(List.of(column1)));
        assertThat(diff.changedColumns(), is(empty()));
    }

    @Test
    void changedColumnsAreReflectedInDiff() {
        CatalogTableColumnDescriptor column1 = someColumn("a");

        var schema1 = new FullTableSchema(1, 1, TABLE_NAME1, List.of(column1));
        var schema2 = new FullTableSchema(2, 1, TABLE_NAME1,
                List.of(new CatalogTableColumnDescriptor("a", ColumnType.STRING, true, 0, 0, 10, DefaultValue.constant(null)))
        );

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        List<ColumnDefinitionDiff> changedColumns = diff.changedColumns();
        assertThat(changedColumns, is(hasSize(1)));
    }

    @Test
    void changedNameIsReflected() {
        CatalogTableColumnDescriptor column = someColumn("a");

        var schema1 = new FullTableSchema(1, 1, TABLE_NAME1, List.of(column));
        var schema2 = new FullTableSchema(1, 1, TABLE_NAME2, List.of(column));

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.nameDiffers(), is(true));
    }
}
