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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition.Builder;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class InvalidDefinitionTest {
    @Test
    void zone() {
        assertThrows(NullPointerException.class,
                () -> ZoneDefinition.builder(null).build(),
                "Zone name must not be null.");

        assertThrows(IllegalArgumentException.class,
                () -> ZoneDefinition.builder("").build(),
                "Zone name must not be blank.");

        assertThrows(NullPointerException.class,
                () -> ZoneDefinition.builder("zone").engine(null).build(),
                "Engine must not be null.");

        assertThrows(NullPointerException.class,
                () -> ZoneDefinition.builder("zone").partitions(null).build(),
                "Number of partitions must not be null.");

        assertThrows(NullPointerException.class,
                () -> ZoneDefinition.builder("zone").replicas(null).build(),
                "Number of replicas must not be null.");
    }

    @Test
    void names() {
        assertThrows(NullPointerException.class,
                () -> TableDefinition.builder(null).build(),
                "Table name must not be null.");

        assertThrows(IllegalArgumentException.class,
                () -> TableDefinition.builder("").build(),
                "Table name must not be blank.");
    }

    @Test
    void columns() {
        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns((ColumnDefinition[]) null).build(),
                "Columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns((List<ColumnDefinition>) null).build(),
                "Columns list must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns((ColumnDefinition) null).build(),
                "Column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns(Collections.singletonList(null)).build(),
                "Column must not be null.");
    }

    @Test
    void colocateBy() {
        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy((String[]) null).build(),
                "Colocation columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy((List<String>) null).build(),
                "Colocation columns list must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy((String) null).build(),
                "Colocation column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy(Collections.singletonList(null)).build(),
                "Colocation column must not be null.");
    }

    @Test
    void primaryKey() {
        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey((String[]) null).build(),
                "Primary key columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey((String) null).build(),
                "Primary key column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(null, (ColumnSorted[]) null).build(),
                "Primary key index type must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(IndexType.DEFAULT, (ColumnSorted[]) null).build(),
                "Primary key columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(IndexType.DEFAULT, (ColumnSorted) null).build(),
                "Primary key column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(null, List.of()).build(),
                "Primary key index type must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(IndexType.DEFAULT, (List<ColumnSorted>) null).build(),
                "Primary key columns list must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(IndexType.DEFAULT, Collections.singletonList(null)).build(),
                "Primary key column must not be null.");
    }

    @Test
    void index() {
        assertThrows(NullPointerException.class,
                () -> tableBuilder().index((String[]) null).build(),
                "Index columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index((String) null).build(),
                "Index column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, null, (ColumnSorted[]) null).build(),
                "Index type must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, IndexType.DEFAULT, (ColumnSorted[]) null).build(),
                "Index columns array must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, IndexType.DEFAULT, (ColumnSorted) null).build(),
                "Index column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, null, List.of()).build(),
                "Index type must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, IndexType.DEFAULT, (List<ColumnSorted>) null).build(),
                "Index columns list must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, IndexType.DEFAULT, Collections.singletonList(null)).build(),
                "Index column must not be null.");
    }

    @Test
    void columnDefinition() {
        assertThrows(NullPointerException.class,
                () -> ColumnDefinition.column(null, (ColumnType<?>) null),
                "Column name must not be null.");

        assertThrows(IllegalArgumentException.class,
                () -> ColumnDefinition.column("", (ColumnType<?>) null),
                "Column name must not be blank.");

        assertThrows(NullPointerException.class,
                () -> ColumnDefinition.column(null, (String) null),
                "Column name must not be null.");

        assertThrows(IllegalArgumentException.class,
                () -> ColumnDefinition.column("", (String) null),
                "Column name must not be blank.");

    }

    private static Builder tableBuilder() {
        return TableDefinition.builder("table");
    }
}
