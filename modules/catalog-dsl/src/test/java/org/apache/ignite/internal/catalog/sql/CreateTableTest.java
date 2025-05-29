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

import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;

class CreateTableTest {
    @Test
    void ifNotExists() {
        Query query1 = createTable().ifNotExists().name(QualifiedName.parse("table1")).addColumn("col1", INTEGER);
        String sql = query1.toString();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS PUBLIC.TABLE1 (COL1 INT);"));
    }

    @Test
    void names() {
        Query query1 = createTable().name(QualifiedName.parse("table1")).addColumn("col1", INTEGER);
        String sql = query1.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT);"));

        Query query2 = createTable().name(QualifiedName.parse("public.table1")).addColumn("col1", INTEGER);
        sql = query2.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT);"));

        Query query3 = createTable().name(QualifiedName.parse("PUBLIC.\"Tabl e\"")).addColumn("col1", INTEGER);
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.\"Tabl e\" (COL1 INT);"));

        Query query4 = createTable().name(QualifiedName.parse("\"PUB lic\".\"Tabl e\"")).addColumn("col1", INTEGER);
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE \"PUB lic\".\"Tabl e\" (COL1 INT);"));

        Query query5 = createTable().name(QualifiedName.parse("\"PUB lic\".\"MyTable\"")).addColumn("col1", INTEGER);
        sql = query5.toString();
        assertThat(sql, is("CREATE TABLE \"PUB lic\".\"MyTable\" (COL1 INT);"));
    }

    @Test
    void columns() {
        Query query3 = createTable().name(QualifiedName.fromSimple("table1"))
                .addColumn("col", INTEGER);
        String sql = query3.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL INT);"));

        Query query2 = createTable().name(QualifiedName.fromSimple("table1"))
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER);
        sql = query2.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT, COL2 INT);"));
    }

    @Test
    void primaryKey() {
        Query query5 = createTable().name(QualifiedName.fromSimple("table1"))
                .addColumn("col", INTEGER)
                .primaryKey(List.of("col"));
        String sql = query5.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL INT, PRIMARY KEY (COL));"));

        Query query4 = createTable().name(QualifiedName.fromSimple("table1"))
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .primaryKey(List.of("col1", "col2"));
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT, COL2 INT, PRIMARY KEY (COL1, COL2));"));

        Query query3 = createTable().name(QualifiedName.fromSimple("table1"))
                .addColumn("col1", INTEGER)
                .primaryKey(IndexType.SORTED, List.of(ColumnSorted.column("col1", SortOrder.ASC_NULLS_FIRST)));
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT, PRIMARY KEY USING SORTED (COL1 ASC NULLS FIRST));"));
    }

    @Test
    void colocateBy() {
        Query query5 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .colocateBy("col1");
        String sql = query5.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT) COLOCATE BY (COL1);"));

        Query query4 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .colocateBy("col1", "col2");
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT) COLOCATE BY (COL1, COL2);"));

        Query query3 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .colocateBy("col1", "col2");
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT) COLOCATE BY (COL1, COL2);"));
    }

    @Test
    void withOptions() {
        Query query1 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .zone("zone1");
        String sql = query1.toString(); // zone param is lowercase
        assertThat(sql, is("CREATE TABLE PUBLIC.TABLE1 (COL1 INT) ZONE ZONE1;")); // zone result is uppercase
    }

    @Test
    void index() {
        Query query3 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.SORTED,
                        List.of(
                                ColumnSorted.column("col1"),
                                ColumnSorted.column("COL2_UPPER", SortOrder.DESC_NULLS_LAST)
                        )
                );
        String sql = query3.toString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS IX_TEST1 ON PUBLIC.TABLE1 USING SORTED (COL1, COL2_UPPER DESC NULLS LAST);"));

        Query query2 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.HASH, List.of(ColumnSorted.column("col1")));
        sql = query2.toString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS IX_TEST1 ON PUBLIC.TABLE1 USING HASH (COL1);"));

        Query query4 = createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.HASH, List.of(ColumnSorted.column("col1", SortOrder.DEFAULT)));
        sql = query4.toString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS IX_TEST1 ON PUBLIC.TABLE1 USING HASH (COL1);"));

        SortOrder[] invalidSortOrders = {
                SortOrder.DESC,
                SortOrder.DESC_NULLS_FIRST,
                SortOrder.DESC_NULLS_LAST,
                SortOrder.ASC,
                SortOrder.ASC_NULLS_FIRST,
                SortOrder.ASC_NULLS_LAST,
                SortOrder.NULLS_FIRST,
                SortOrder.NULLS_LAST
        };

        for (SortOrder order : invalidSortOrders) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> createTable().name(QualifiedName.fromSimple("table1")).addColumn("col1", INTEGER)
                            .addIndex("ix_test1", IndexType.HASH, List.of(ColumnSorted.column("col1", order))),
                    "Index columns must not define a sort order in hash indexes."
            );
        }
    }

    private static CreateTableImpl createTable() {
        return new CreateTableImpl(null);
    }
}
