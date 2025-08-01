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

import static org.apache.ignite.catalog.ColumnSorted.column;
import static org.apache.ignite.catalog.ColumnType.BIGINT;
import static org.apache.ignite.catalog.ColumnType.BOOLEAN;
import static org.apache.ignite.catalog.ColumnType.DATE;
import static org.apache.ignite.catalog.ColumnType.DECIMAL;
import static org.apache.ignite.catalog.ColumnType.DOUBLE;
import static org.apache.ignite.catalog.ColumnType.FLOAT;
import static org.apache.ignite.catalog.ColumnType.INT16;
import static org.apache.ignite.catalog.ColumnType.INT32;
import static org.apache.ignite.catalog.ColumnType.INT64;
import static org.apache.ignite.catalog.ColumnType.INT8;
import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.ColumnType.REAL;
import static org.apache.ignite.catalog.ColumnType.SMALLINT;
import static org.apache.ignite.catalog.ColumnType.TIME;
import static org.apache.ignite.catalog.ColumnType.TIMESTAMP;
import static org.apache.ignite.catalog.ColumnType.TINYINT;
import static org.apache.ignite.catalog.ColumnType.UUID;
import static org.apache.ignite.catalog.ColumnType.VARBINARY;
import static org.apache.ignite.catalog.ColumnType.VARCHAR;
import static org.apache.ignite.internal.catalog.sql.ColumnTypeImpl.wrap;
import static org.apache.ignite.internal.catalog.sql.IndexColumnImpl.parseColumn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;

class QueryPartTest {
    @Test
    void simpleNamePart() {
        assertThat(sql(Name.simple("a")), is("A"));
        assertThat(sql(Name.simple("A")), is("A"));
        assertThat(sql(Name.simple("aBCd")), is("ABCD"));
        assertThat(sql(Name.simple("\"aBcD\"")), is("\"aBcD\""));

        assertThat(sql(Name.simple("a.b")), is("\"a.b\""));
        assertThat(sql(Name.simple(".a.b")), is("\".a.b\""));

        // whitespace
        assertThat(sql(Name.simple("a b")), is("\"a b\""));
        assertThat(sql(Name.simple("a ")), is("\"a \""));
        assertThat(sql(Name.simple(" a")), is("\" a\""));
        assertThat(sql(Name.simple("a b ")), is("\"a b \""));
        assertThat(sql(Name.simple(" a b ")), is("\" a b \""));
        assertThat(sql(Name.simple(" aXb ")), is("\" aXb \""));

        // underscore
        assertThat(sql(Name.simple("_a")), is("_A"));
        assertThat(sql(Name.simple("_aB")), is("_AB"));
        assertThat(sql(Name.simple("_a_B_c")), is("_A_B_C"));
        assertThat(sql(Name.simple("_a B")), is("\"_a B\""));
        assertThat(sql(Name.simple("_a b")), is("\"_a b\""));
    }

    @Test
    void qualifiedNamePart() {
        assertThat(sql(Name.qualified(QualifiedName.fromSimple("a"))), is("PUBLIC.A"));
        assertThat(sql(Name.qualified(QualifiedName.parse("a.b"))), is("A.B"));
        assertThat(sql(Name.qualified(QualifiedName.parse("\"Ab\""))), is("PUBLIC.\"Ab\""));
        assertThat(sql(Name.qualified(QualifiedName.parse("\"a b\""))), is("PUBLIC.\"a b\""));
        assertThat(sql(Name.qualified(QualifiedName.parse("\"a b\".\"c d\""))), is("\"a b\".\"c d\""));
        assertThat(sql(Name.qualified(QualifiedName.of("\"a b\"", "\"c d\""))), is("\"a b\".\"c d\""));
    }

    @Test
    void colocatePart() {
        Colocate colocate = new Colocate("a");
        assertThat(sql(colocate), is("COLOCATE BY (A)"));

        colocate = new Colocate("a", "b");
        assertThat(sql(colocate), is("COLOCATE BY (A, B)"));
    }

    @Test
    void columnPart() {
        Column column = new Column("a", wrap(VARCHAR));
        assertThat(sql(column), is("A VARCHAR"));

        column = new Column("a", wrap(ColumnType.varchar(3)));
        assertThat(sql(column), is("A VARCHAR(3)"));

        column = new Column("a", wrap(ColumnType.decimal(2, 3)));
        assertThat(sql(column), is("A DECIMAL(2, 3)"));
    }

    @Test
    void columnTypePart() {
        assertThat(sql(wrap(BOOLEAN)), is("BOOLEAN"));
        assertThat(sql(wrap(TINYINT)), is("TINYINT"));
        assertThat(sql(wrap(SMALLINT)), is("SMALLINT"));
        assertThat(sql(wrap(INT8)), is("TINYINT"));
        assertThat(sql(wrap(INT16)), is("SMALLINT"));
        assertThat(sql(wrap(INT32)), is("INT"));
        assertThat(sql(wrap(INT64)), is("BIGINT"));
        assertThat(sql(wrap(INTEGER)), is("INT"));
        assertThat(sql(wrap(BIGINT)), is("BIGINT"));
        assertThat(sql(wrap(REAL)), is("REAL"));
        assertThat(sql(wrap(FLOAT)), is("REAL"));
        assertThat(sql(wrap(DOUBLE)), is("DOUBLE"));
        assertThat(sql(wrap(VARCHAR)), is("VARCHAR"));
        assertThat(sql(wrap(ColumnType.varchar(1))), is("VARCHAR(1)"));
        assertThat(sql(wrap(VARBINARY)), is("VARBINARY"));
        assertThat(sql(wrap(ColumnType.varbinary(1))), is("VARBINARY(1)"));
        assertThat(sql(wrap(TIME)), is("TIME"));
        assertThat(sql(wrap(ColumnType.time(1))), is("TIME(1)"));
        assertThat(sql(wrap(TIMESTAMP)), is("TIMESTAMP"));
        assertThat(sql(wrap(ColumnType.timestamp(1))), is("TIMESTAMP(1)"));
        assertThat(sql(wrap(DATE)), is("DATE"));
        assertThat(sql(wrap(DECIMAL)), is("DECIMAL"));
        assertThat(sql(wrap(ColumnType.decimal(1, 2))), is("DECIMAL(1, 2)"));
        assertThat(sql(wrap(UUID)), is("UUID"));
    }

    @Test
    void columnTypeOptionsPart() {
        assertThat(sql(wrap(INTEGER)), is("INT"));
        assertThat(sql(wrap(INTEGER.notNull())), is("INT NOT NULL"));
        assertThat(sql(wrap(INTEGER.defaultValue(1))), is("INT DEFAULT 1"));
        assertThat(sql(wrap(VARCHAR.defaultValue("s"))), is("VARCHAR DEFAULT 's'")); // default in single quotes
        assertThat(sql(wrap(INTEGER.defaultExpression("gen_expr"))), is("INT DEFAULT gen_expr"));
        assertThat(sql(wrap(INTEGER.notNull().defaultValue(1))), is("INT NOT NULL DEFAULT 1"));
        assertThat(sql(wrap(ColumnType.decimal(2, 3).defaultValue(BigDecimal.ONE).notNull())), is("DECIMAL(2, 3) NOT NULL DEFAULT 1"));
        assertThat(sql(wrap(INTEGER.defaultValue(1).defaultExpression("gen_expr"))), is("INT DEFAULT 1"));
    }

    @Test
    void constraintPart() {
        Constraint constraint = new Constraint().primaryKey(column("a"));
        assertThat(sql(constraint), is("PRIMARY KEY (A)"));

        constraint = new Constraint().primaryKey(IndexType.SORTED, List.of(column("a")));
        assertThat(sql(constraint), is("PRIMARY KEY USING SORTED (A)"));

        constraint = new Constraint().primaryKey(column("a"), column("b"));
        assertThat(sql(constraint), is("PRIMARY KEY (A, B)"));

        constraint = new Constraint().primaryKey(IndexType.SORTED, List.of(column("a"), column("b")));
        assertThat(sql(constraint), is("PRIMARY KEY USING SORTED (A, B)"));
    }

    @Test
    void queryPartCollection() {
        QueryPartCollection<Name> collection = QueryPartCollection.partsList(Name.simple("a"), Name.simple("b"));

        assertThat(sql(collection), is("A, B"));
    }

    @Test
    void indexColumnPart() {
        IndexColumnImpl column = IndexColumnImpl.wrap(column("col1"));
        assertThat(sql(column), is("COL1"));

        column = IndexColumnImpl.wrap(column("col1", SortOrder.ASC_NULLS_FIRST));
        assertThat(sql(column), is("COL1 ASC NULLS FIRST"));

        column = IndexColumnImpl.wrap(column("col1", SortOrder.DESC_NULLS_LAST));
        assertThat(sql(column), is("COL1 DESC NULLS LAST"));
    }

    @Test
    void indexColumnParseSorted() {
        assertThat(parseColumn("col1"), is(column("col1", SortOrder.DEFAULT)));
        assertThat(parseColumn("COL2_UPPER_CASE ASC"), is(column("COL2_UPPER_CASE", SortOrder.ASC)));
        assertThat(parseColumn("col3 ASC    nUlls First  "), is(column("col3", SortOrder.ASC_NULLS_FIRST)));
        assertThat(parseColumn(" col4   asc  nulls  last "), is(column("col4", SortOrder.ASC_NULLS_LAST)));
        assertThat(parseColumn("col5 desc"), is(column("col5", SortOrder.DESC)));
        assertThat(parseColumn("col6 desc nulls first"), is(column("col6", SortOrder.DESC_NULLS_FIRST)));
        assertThat(parseColumn("col7 desc nulls last"), is(column("col7", SortOrder.DESC_NULLS_LAST)));
        assertThat(parseColumn("col8 nulls first"), is(column("col8", SortOrder.NULLS_FIRST)));
        assertThat(parseColumn("col9 nulls last"), is(column("col9", SortOrder.NULLS_LAST)));
    }

    @Test
    void indexColumnParseSortedWrongOrder() {
        assertThat(parseColumn("col1 nulls first asc"), is(column("col1", SortOrder.NULLS_FIRST)));
        assertThat(parseColumn("col2 nulls last desc"), is(column("col2", SortOrder.NULLS_LAST)));
        assertThat(parseColumn("col3 desc nulls"), is(column("col3", SortOrder.DESC)));
        assertThat(parseColumn("col4 desc last nulls"), is(column("col4", SortOrder.DESC)));
        assertThat(parseColumn("col5 nulls asc first"), is(column("col5")));
        assertThat(parseColumn("col6 first nulls"), is(column("col6")));
    }

    @Test
    void indexColumnPareIncorrectSortOrder() {
        assertThat(parseColumn("col1 unexpectedKeyword"), is(column("col1")));
        assertThat(parseColumn("col2 nulls_first"), is(column("col2")));
        assertThat(parseColumn("col3 descnullslast"), is(column("col3")));
    }

    private static String sql(QueryPart part) {
        return new QueryContext().visit(part).getSql();
    }
}
