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
import org.apache.ignite.catalog.Options;
import org.junit.jupiter.api.Test;

class CreateTableTest {
    @Test
    void ifNotExists() {
        String sql = createTable().ifNotExists().name("table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS table1 (col1 int);"));

        // quote identifiers
        sql = createTableQuoted().ifNotExists().name("table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE IF NOT EXISTS \"table1\" (\"col1\" int);"));
    }

    @Test
    void names() {
        String sql = createTable().name("", "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int);"));

        sql = createTable().name(null, "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int);"));

        sql = createTable().name("public", "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE public.table1 (col1 int);"));

        // quote identifiers
        sql = createTableQuoted().name("", "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int);"));

        sql = createTableQuoted().name(null, "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int);"));

        sql = createTableQuoted().name("public", "table1").addColumn("col1", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE \"public\".\"table1\" (\"col1\" int);"));
    }

    @Test
    void invalidNames() {
        assertThrows(NullPointerException.class, () -> createTable().name((String[]) null));

        assertThrows(IllegalArgumentException.class, () -> createTable().name("table;1--test\n\r\t;"));
    }

    @Test
    void columns() {
        String sql = createTable().name("table1")
                .addColumn("col", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col int);"));

        sql = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int);"));

        // quote identifiers
        sql = createTableQuoted().name("table1")
                .addColumn("col", INTEGER).toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col\" int);"));

        sql = createTableQuoted().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, \"col2\" int);"));
    }

    @Test
    void primaryKey() {
        String sql = createTable().name("table1")
                .addColumn("col", INTEGER)
                .primaryKey("col")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col int, PRIMARY KEY (col));"));

        sql = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .primaryKey("col1, col2")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, col2 int, PRIMARY KEY (col1, col2));"));

        sql = createTable().name("table1")
                .addColumn("col1", INTEGER)
                .primaryKey(IndexType.SORTED, "col1 ASC    nUlls First  ")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int, PRIMARY KEY USING SORTED (col1 asc nulls first));"));

        // quote identifiers
        sql = createTableQuoted().name("table1")
                .addColumn("col", INTEGER)
                .primaryKey("col")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col\" int, PRIMARY KEY (\"col\"));"));

        sql = createTableQuoted().name("table1")
                .addColumn("col1", INTEGER)
                .addColumn("col2", INTEGER)
                .primaryKey("col1, col2")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, \"col2\" int, PRIMARY KEY (\"col1\", \"col2\"));"));

        sql = createTableQuoted().name("table1")
                .addColumn("col1", INTEGER)
                .primaryKey(IndexType.SORTED, "col1 ASC nUlls First   ")
                .toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int, PRIMARY KEY USING SORTED (\"col1\" asc nulls first));"));
    }

    @Test
    void colocateBy() {
        String sql = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1").toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1);"));

        sql = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1", "col2").toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1, col2);"));

        sql = createTable().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1, col2").toSqlString();
        assertThat(sql, is("CREATE TABLE table1 (col1 int) COLOCATE BY (col1, col2);"));

        // quote identifiers
        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1").toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int) COLOCATE BY (\"col1\");"));

        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1", "col2").toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int) COLOCATE BY (\"col1\", \"col2\");"));

        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .colocateBy("col1, col2").toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int) COLOCATE BY (\"col1\", \"col2\");"));
    }

    @Test
    void withOptions() {
        String sql = createTable().name("table1").addColumn("col1", INTEGER)
                .zone("zone1").toSqlString(); // zone param is lowercase
        assertThat(sql, is("CREATE TABLE table1 (col1 int) WITH PRIMARY_ZONE='ZONE1';")); // zone result is uppercase

        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .zone("zone1").toSqlString();
        assertThat(sql, is("CREATE TABLE \"table1\" (\"col1\" int) WITH PRIMARY_ZONE='ZONE1';"));
    }

    @Test
    void index() {
        String sql = createTable().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", "col1, COL2_UPPER desc nulls last").toSqlString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 (col1, COL2_UPPER desc nulls last);"));

        sql = createTable().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.HASH, "col1").toSqlString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS ix_test1 ON table1 USING HASH (col1);"));

        // quote identifiers
        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", "col1, COL2_UPPER desc nulls last").toSqlString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS \"ix_test1\" ON \"table1\" (\"col1\", \"COL2_UPPER\" desc nulls last);"));

        sql = createTableQuoted().name("table1").addColumn("col1", INTEGER)
                .addIndex("ix_test1", IndexType.HASH, "col1").toSqlString();
        assertThat(sql, endsWith("CREATE INDEX IF NOT EXISTS \"ix_test1\" ON \"table1\" USING HASH (\"col1\");"));
    }

    private static CreateTableImpl createTable() {
        return new CreateTableImpl(null, Options.DEFAULT);
    }

    private static CreateTableImpl createTableQuoted() {
        return new CreateTableImpl(null, Options.builder().quoteIdentifiers().build());
    }
}
