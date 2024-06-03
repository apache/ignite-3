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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.Query;
import org.junit.jupiter.api.Test;

class CreateTableTest {
    @Test
    void ifNotExists() {
        Query query1 = createTable().ifNotExists().name("table1").addColumn("col1", INTEGER);
        String sql = query1.toString();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS table1 (col1 int);"));
    }

    @Test
    void names() {
        Query query5 = createTable().name("", "table1").addColumn("col1", INTEGER);
        String sql = query5.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int);"));

        Query query4 = createTable().name(null, "table1").addColumn("col1", INTEGER);
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int);"));

        Query query3 = createTable().name("public", "table1").addColumn("col1", INTEGER);
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE public.table1 (col1 int);"));
    }

    @Test
    void invalidNames() {
        assertThrows(NullPointerException.class, () -> createTable().name((String[]) null));

        assertThrows(IllegalArgumentException.class, () -> createTable().name("table;1--test\n\r\t;"));
    }

    @Test
    void columns() {
        Query query3 = createTable().name("table1")
                .addColumn("col", INTEGER);
        String sql = query3.toString();
        assertThat(sql, is("CREATE TABLE table1 (col int);"));

        Query query2 = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER);
        sql = query2.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int);"));
    }

    @Test
    void primaryKey() {
        Query query5 = createTable().name("table1")
                .addColumn("col", INTEGER)
                .primaryKey("col");
        String sql = query5.toString();
        assertThat(sql, is("CREATE TABLE table1 (col int, PRIMARY KEY (col));"));

        Query query4 = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .primaryKey("col1, col2");
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int, PRIMARY KEY (col1, col2));"));

        Query query3 = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .primaryKey(IndexType.SORTED, "col1 ASC    nUlls First  ");
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, PRIMARY KEY USING SORTED (col1 asc nulls first));"));
    }

    @Test
    void colocateBy() {
        Query query5 = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1");
        String sql = query5.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1);"));

        Query query4 = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1", "col2");
        sql = query4.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1, col2);"));

        Query query3 = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1, col2");
        sql = query3.toString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1, col2);"));
    }

    @Test
    void withOptions() {
        Query query1 = createTable().name("table1").addColumn("col1", INTEGER)
                .zone("zone1");
        String sql = query1.toString(); // zone param is lowercase
        assertThat(sql, is("CREATE TABLE table1 (col1 int) WITH PRIMARY_ZONE='ZONE1';")); // zone result is uppercase
    }

    @Test
    void index() {
        Query query3 = createTable().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", "col1, COL2_UPPER desc nulls last");
        String sql = query3.toString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 (col1, COL2_UPPER desc nulls last);"));

        Query query2 = createTable().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.HASH, "col1");
        sql = query2.toString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 USING HASH (col1);"));
    }

    private static CreateTableImpl createTable() {
        return new CreateTableImpl(null);
    }
}
