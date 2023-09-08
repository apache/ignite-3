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
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

class FullTableSchemaTest {
    @Test
    void sameSchemasHaveEmptyDiff() {
        CatalogTableColumnDescriptor column = someColumn("a");
        CatalogIndexDescriptor index = someIndex(1, "ind_a");

        var schema1 = new FullTableSchema(1, 1, List.of(column), List.of(index));
        var schema2 = new FullTableSchema(2, 1, List.of(column), List.of(index));

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.isEmpty(), is(true));
    }

    private static CatalogHashIndexDescriptor someIndex(int id, String name) {
        return new CatalogHashIndexDescriptor(id, name, 1, true, List.of("a"));
    }

    private static CatalogTableColumnDescriptor someColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, ColumnType.INT32, true, 0, 0, 0, DefaultValue.constant(null));
    }

    @Test
    void addedRemovedColumnsAreReflectedInDiff() {
        CatalogTableColumnDescriptor column1 = someColumn("a");
        CatalogTableColumnDescriptor column2 = someColumn("b");
        CatalogTableColumnDescriptor column3 = someColumn("c");

        var schema1 = new FullTableSchema(1, 1, List.of(column1, column2), List.of());
        var schema2 = new FullTableSchema(2, 1, List.of(column2, column3), List.of());

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.addedColumns(), is(List.of(column3)));
        assertThat(diff.removedColumns(), is(List.of(column1)));
        assertThat(diff.changedColumns(), is(empty()));
    }

    @Test
    void changedColumnsAreReflectedInDiff() {
        CatalogTableColumnDescriptor column1 = someColumn("a");

        var schema1 = new FullTableSchema(1, 1, List.of(column1), List.of());
        var schema2 = new FullTableSchema(2, 1,
                List.of(new CatalogTableColumnDescriptor("a", ColumnType.STRING, true, 0, 0, 10, DefaultValue.constant(null))),
                List.of()
        );

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.isEmpty(), is(false));

        List<ColumnDefinitionDiff> changedColumns = diff.changedColumns();
        assertThat(changedColumns, is(hasSize(1)));
    }

    @Test
    void addedRemovedIndexesAreReflectedInDiff() {
        CatalogIndexDescriptor index1 = someIndex(1, "a");
        CatalogIndexDescriptor index2 = someIndex(2, "b");
        CatalogIndexDescriptor index3 = someIndex(3, "c");

        var schema1 = new FullTableSchema(1, 1, List.of(someColumn("a")), List.of(index1, index2));
        var schema2 = new FullTableSchema(2, 1, List.of(someColumn("a")), List.of(index2, index3));

        TableDefinitionDiff diff = schema2.diffFrom(schema1);

        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.addedIndexes(), is(List.of(index3)));
        assertThat(diff.removedIndexes(), is(List.of(index1)));
    }
}
