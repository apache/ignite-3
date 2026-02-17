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

package org.apache.ignite.internal.sql.engine.sql;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlTablePropertyKey.MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlTablePropertyKey.STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test suite to verify parsing of the DDL command.
 */
public class SqlDdlParserTest extends AbstractParserTest {
    /**
     * Very simple case where only table name and a few columns are presented.
     */
    @Test
    public void createTableSimpleCase() {
        String query = "create table my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" ("
                + "\"ID\" INTEGER, "
                + "\"VAL\" VARCHAR)"
        );
    }

    /**
     * Parsing of CREATE TABLE with function identifier as a default expression.
     */
    @Test
    public void createTableAutogenFuncDefault() {
        String query = "create table my_table(id uuid default rand_uuid primary key, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "Column with function's identifier as default",
                SqlColumnDeclaration.class,
                col -> "ID".equals(col.name.getSimple())
                        && col.expression instanceof SqlIdentifier
                        && "RAND_UUID".equals(((SqlIdentifier) col.expression).getSimple())
        )));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"ID\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" ("
                + "PRIMARY KEY (\"ID\"), "
                + "\"ID\" UUID DEFAULT (\"RAND_UUID\"), "
                + "\"VAL\" VARCHAR)"
        );
    }

    /**
     * Parsing of CREATE TABLE with a literal as a default expression.
     */
    @Test
    public void createTableWithDefaultLiteral() {
        String query = "CREATE TABLE my_table("
                + "id BIGINT DEFAULT 1, "
                + "valdate DATE DEFAULT DATE '2001-12-21',"
                + "valdate2 DATE DEFAULT '2001-12-21',"
                + "valtime TIME DEFAULT TIME '11:22:33.444',"
                + "valtime2 TIME DEFAULT '11:22:33.444',"
                + "valts TIMESTAMP DEFAULT TIMESTAMP '2001-12-21 11:22:33.444',"
                + "valts2 TIMESTAMP DEFAULT '2001-12-21 11:22:33.444',"
                + "valbin VARBINARY DEFAULT x'ff',"
                + "valstr VARCHAR DEFAULT 'string'"
                + ")";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.columnList(), hasColumnWithDefault("ID", SqlNumericLiteral.class, "1"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALSTR", SqlCharStringLiteral.class, "string"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALBIN", SqlBinaryStringLiteral.class, "11111111"));

        assertThat(createTable.columnList(), hasColumnWithDefault("VALDATE", SqlUnknownLiteral.class, "2001-12-21"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALTIME", SqlUnknownLiteral.class, "11:22:33.444"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALTS", SqlUnknownLiteral.class, "2001-12-21 11:22:33.444"));

        assertThat(createTable.columnList(), hasColumnWithDefault("VALDATE2", SqlCharStringLiteral.class, "2001-12-21"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALTIME2", SqlCharStringLiteral.class, "11:22:33.444"));
        assertThat(createTable.columnList(), hasColumnWithDefault("VALTS2", SqlCharStringLiteral.class, "2001-12-21 11:22:33.444"));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" "
                + "(\"ID\" BIGINT DEFAULT (1), "
                + "\"VALDATE\" DATE DEFAULT (DATE '2001-12-21'), "
                + "\"VALDATE2\" DATE DEFAULT ('2001-12-21'), "
                + "\"VALTIME\" TIME DEFAULT (TIME '11:22:33.444'), "
                + "\"VALTIME2\" TIME DEFAULT ('11:22:33.444'), "
                + "\"VALTS\" TIMESTAMP DEFAULT (TIMESTAMP '2001-12-21 11:22:33.444'), "
                + "\"VALTS2\" TIMESTAMP DEFAULT ('2001-12-21 11:22:33.444'), "
                + "\"VALBIN\" VARBINARY DEFAULT (X'FF'), "
                + "\"VALSTR\" VARCHAR DEFAULT ('string'))"
        );
    }

    /**
     * Parsing of CREATE TABLE with a expression in DEFAULT.
     */
    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "1+2; 1 + 2",
            "a=100; \"A\" = 100",
            "RANDOM(); \"RANDOM\"()",
            "LENGTH('abc'); \"LENGTH\"('abc')",
            "LENGTH('abc') + 1; \"LENGTH\"('abc') + 1",
            "42 +LENGTH('abc') + 1; 42 + \"LENGTH\"('abc') + 1",
            "LENGTH(CAST(1234 AS VARCHAR)); \"LENGTH\"(CAST(1234 AS VARCHAR))",
            "LENGTH(1234::VARCHAR); \"LENGTH\"(1234 :: VARCHAR)",
    })
    public void createTableWithDefaultExpression(String expr, String normExpr) {
        {
            SqlNode node = parse(format("CREATE TABLE t(id INT DEFAULT {} PRIMARY KEY, val INT)", expr));
            assertThat(node, instanceOf(IgniteSqlCreateTable.class));

            IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

            // Use a SQL string in error message because matchers are not good at producing a meaningful error
            // when an object does not have its own toString method
            assertThat(unparse(createTable.columnList()), createTable.columnList(), hasColumnWithDefaultExpr("ID",
                    SqlBasicCall.class,
                    "Unexpected expression",
                    (dflt) -> normExpr.equals(unparse(dflt)))
            );
            // Output expression is parenthesized by the unparse
            expectUnparsed(node, "CREATE TABLE \"T\" ("
                    + "PRIMARY KEY (\"ID\"), "
                    + "\"ID\" INTEGER DEFAULT (" + normExpr + "), "
                    + "\"VAL\" INTEGER"
                    + ")"
            );
        }

        {
            SqlNode node = parse(format("CREATE TABLE t(id INT PRIMARY KEY, val INT DEFAULT {})", expr));
            assertThat(node, instanceOf(IgniteSqlCreateTable.class));

            IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;
            assertThat(unparse(createTable.columnList()), createTable.columnList(), hasColumnWithDefaultExpr("VAL",
                    SqlBasicCall.class,
                    "Unexpected expression",
                    (dflt) -> normExpr.equals(unparse(dflt)))
            );
            expectUnparsed(node, "CREATE TABLE \"T\" ("
                    + "PRIMARY KEY (\"ID\"), "
                    + "\"ID\" INTEGER, "
                    + "\"VAL\" INTEGER DEFAULT (" + normExpr + ")"
                    + ")"
            );
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "CREATE TABLE t(id int DEFAULT (SELECT count(col) FROM t), val int); Query expression encountered in illegal context",
            "CREATE TABLE t(id int DEFAULT (SELECT 100), val int); Query expression encountered in illegal context",
            "CREATE TABLE t(id int DEFAULT (UPDATE t SET col=1) PRIMARY KEY, val int); Incorrect syntax near the keyword 'UPDATE'",
            "CREATE TABLE t(id int DEFAULT (INSERT INTO t VALUES(1)), val int); Incorrect syntax near the keyword 'INSERT'",
            "CREATE TABLE t(id int DEFAULT (DELETE FROM t WHERE col = 1), val int); Incorrect syntax near the keyword 'DELETE'",

            "ALTER TABLE t ADD COLUMN val int DEFAULT (SELECT count(*) FROM xyz); Query expression encountered in illegal context",
            "ALTER TABLE t ADD COLUMN val int DEFAULT (UPDATE t SET col=1); Incorrect syntax near the keyword 'UPDATE'",
            "ALTER TABLE t ADD COLUMN val int DEFAULT (INSERT INTO t VALUES(1)); Incorrect syntax near the keyword 'INSERT",
            "ALTER TABLE t ADD COLUMN val int DEFAULT (DELETE FROM t WHERE col = 1); Incorrect syntax near the keyword 'DELETE'",

            "ALTER TABLE t ALTER COLUMN val SET DEFAULT (SELECT count(*) FROM xyz); Query expression encountered in illegal context",
            "ALTER TABLE t ALTER COLUMN val SET DEFAULT (SELECT 100); Query expression encountered in illegal context",
            "ALTER TABLE t ALTER COLUMN val SET DEFAULT (UPDATE t SET col=1); Incorrect syntax near the keyword 'UPDATE'",
            "ALTER TABLE t ALTER COLUMN val SET DEFAULT (INSERT INTO t VALUES(1)); Incorrect syntax near the keyword 'INSERT",
            "ALTER TABLE t ALTER COLUMN val SET DEFAULT (DELETE FROM t WHERE col = 1); Incorrect syntax near the keyword 'DELETE'",
    })
    public void rejectNonExpressionsInDefault(String stmt, String error) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                error,
                () -> parse(stmt));
    }

    private <T extends SqlLiteral>  Matcher<Iterable<? super SqlColumnDeclaration>> hasColumnWithDefault(
            String columnName,
            Class<T> nodeType,
            String literalValue
    ) {
        return hasColumnWithDefaultExpr(
                columnName,
                nodeType,
                format("Column with {} as default: columnName={}", nodeType.getCanonicalName(), columnName),
                (lit) -> literalValue.equals(lit.toValue())
        );
    }

    private static <T extends SqlNode> Matcher<Iterable<? super SqlColumnDeclaration>> hasColumnWithDefaultExpr(
            String columnName,
            Class<T> nodeType,
            String message,
            Predicate<T> valCheck
    ) {
        return hasItem(ofTypeMatching(
                message,
                SqlColumnDeclaration.class,
                col -> columnName.equals(col.name.getSimple())
                        && nodeType.isInstance(col.expression)
                        && valCheck.test(nodeType.cast(col.expression))
        ));
    }

    /**
     * Parsing of CREATE TABLE statement with quoted identifiers.
     */
    @Test
    public void createTableQuotedIdentifiers() {
        String query = "create table \"My_Table\"(\"Id\" int, \"Val\" varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("My_Table")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("Id")));
        assertThat(createTable.columnList(), hasItem(columnWithName("Val")));

        expectUnparsed(node, "CREATE TABLE \"My_Table\" "
                + "(\"Id\" INTEGER, "
                + "\"Val\" VARCHAR)"
        );
    }

    /**
     * Parsing of CREATE TABLE statement with IF NOT EXISTS.
     */
    @Test
    public void createTableIfNotExists() {
        String query = "create table if not exists my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(true));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));

        expectUnparsed(node, "CREATE TABLE IF NOT EXISTS \"MY_TABLE\" ("
                + "\"ID\" INTEGER, \"VAL\" VARCHAR)"
        );
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is a shortcut within a column definition.
     */
    @Test
    public void createTableWithPkCase1() {
        String query = "create table my_table(id int primary key, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"ID\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" ("
                + "PRIMARY KEY (\"ID\"), "
                + "\"ID\" INTEGER, "
                + "\"VAL\" VARCHAR)"
        );
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has no name.
     */
    @Test
    public void createTableWithPkCase2() {
        String query = "create table my_table(id int, val varchar, primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint without name containing column \"ID\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" "
                + "(\"ID\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "PRIMARY KEY (\"ID\"))"
        );
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has a name.
     */
    @Test
    public void createTableWithPkCase3() {
        String query = "create table my_table(id int, val varchar, constraint pk_key primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"PK_KEY\" containing column \"ID\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && "PK_KEY".equals(((SqlIdentifier) constraint.getOperandList().get(0)).names.get(0))
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" ("
                + "\"ID\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "CONSTRAINT \"PK_KEY\" PRIMARY KEY (\"ID\"))"
        );
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint consists of several columns.
     */
    @Test
    public void createTableWithPkCase4() {
        String query = "create table my_table(id1 int, id2 int, val varchar, primary key(id1, id2))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with two columns", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID1\"", SqlIdentifier.class, id -> "ID1".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && hasItem(ofTypeMatching("identifier \"ID2\"", SqlIdentifier.class, id -> "ID2".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" ("
                + "\"ID1\" INTEGER, \"ID2\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "PRIMARY KEY (\"ID1\", \"ID2\"))"
        );
    }

    /**
     * Parsing of CREATE TABLE with primary key index type SORTED.
     */
    @Test
    public void createTableWithSortedPk() {
        String query = "create table my_table(id1 int, id2 int, val varchar, primary key using sorted (id1 ASC, id2 DESC))";
        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));

        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint without name containing column \"ID1\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID1\"", SqlIdentifier.class, id -> "ID1".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.SORTED
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" "
                + "(\"ID1\" INTEGER, "
                + "\"ID2\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "PRIMARY KEY USING SORTED (\"ID1\", \"ID2\" DESC))"
        );
    }

    @Test
    public void createTableWithSortedPkAndParticularNullOrdering() {
        String query = "create table my_table(id1 int, id2 int, val varchar, " 
                + "primary key using sorted (id1 ASC NULLS FIRST, id2 DESC NULLS LAST))";
        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));

        SqlNode lastItem = CollectionUtils.last(createTable.columnList());

        assertThat(lastItem, instanceOf((IgniteSqlPrimaryKeyConstraint.class)));

        IgniteSqlPrimaryKeyConstraint pkConstraint = (IgniteSqlPrimaryKeyConstraint) lastItem;

        assertThat(
                pkConstraint.getColumnList().get(0),
                ofTypeMatching("\"ID1\" NULLS FIRST", SqlBasicCall.class, bc -> bc.isA(Set.of(SqlKind.NULLS_FIRST))
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("ID1"))
        );

        assertThat(
                pkConstraint.getColumnList().get(1),
                ofTypeMatching("\"ID2\" DESC NULLS LAST", SqlBasicCall.class, bc -> bc.isA(Set.of(SqlKind.NULLS_LAST))
                        && bc.getOperandList().get(0) instanceof SqlBasicCall
                        && (bc.getOperandList().get(0)).isA(Set.of(SqlKind.DESCENDING))
                        && ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0)).isSimple() 
                        && ((SqlIdentifier) ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0)).getSimple().equals("ID2"))
        );

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" "
                + "(\"ID1\" INTEGER, "
                + "\"ID2\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "PRIMARY KEY USING SORTED (\"ID1\" NULLS FIRST, \"ID2\" DESC NULLS LAST))"
        );
    }

    /**
     * Parsing of CREATE TABLE with primary key index type HASH.
     */
    @Test
    public void createTableWithHashPk() {
        String query = "create table my_table(id int, val varchar, primary key using hash (id))";
        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));
        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint without name containing column \"ID\"", IgniteSqlPrimaryKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.getIndexType() == IgniteSqlPrimaryKeyIndexType.HASH
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" "
                + "(\"ID\" INTEGER, "
                + "\"VAL\" VARCHAR, "
                + "PRIMARY KEY USING HASH (\"ID\"))"
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Encountered \"asc\"",
                () -> parse("create table my_table(id int, val varchar, primary key using hash (id asc))"));
    }

    /**
     * Parsing of CREATE TABLE with primary key index of unsupported type should return an error.
     */
    @Test
    public void createTableWithSomeOtherPkFails() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Encountered \"using\"",
                () -> parse("create table my_table(id int, val varchar, primary key using bad (id))"));
    }

    /**
     * Parsing of CREATE TABLE with specified colocation columns.
     */
    @Test
    public void createTableWithColocationBy() {
        IgniteSqlCreateTable createTable;

        createTable = parseCreateTable(
                "CREATE TABLE MY_TABLE(ID0 INT, ID1 INT, ID2 INT, VAL INT, PRIMARY KEY (ID0, ID1, ID2))"
        );

        assertNull(createTable.colocationColumns());

        createTable = parseCreateTable(
                "CREATE TABLE MY_TABLE(ID0 INT, ID1 INT, ID2 INT, VAL INT, PRIMARY KEY (ID0, ID1, ID2)) COLOCATE BY (ID2, ID1)"
        );

        assertThat(
                createTable.colocationColumns().getList().stream()
                        .map(SqlIdentifier.class::cast)
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toList()),
                equalTo(List.of("ID2", "ID1"))
        );

        expectUnparsed(createTable, "CREATE TABLE \"MY_TABLE\" ("
                + "\"ID0\" INTEGER, \"ID1\" INTEGER, "
                + "\"ID2\" INTEGER, \"VAL\" INTEGER, PRIMARY KEY (\"ID0\", \"ID1\", \"ID2\")"
                + ") COLOCATE BY (\"ID2\", \"ID1\")"
        );

        createTable = parseCreateTable(
                "CREATE TABLE MY_TABLE(ID0 INT, ID1 INT, ID2 INT, VAL INT, PRIMARY KEY (ID0, ID1, ID2)) COLOCATE (ID0)"
        );

        assertThat(
                createTable.colocationColumns().getList().stream()
                        .map(SqlIdentifier.class::cast)
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toList()),
                equalTo(List.of("ID0"))
        );

        // Check uparse 'COLOCATE' and 'WITH' together.
        createTable = parseCreateTable(
                "CREATE TABLE MY_TABLE(ID0 INT, ID1 INT, ID2 INT, VAL INT, PRIMARY KEY (ID0, ID1, ID2)) COLOCATE (ID0)"
        );

        expectUnparsed(createTable, "CREATE TABLE \"MY_TABLE\" ("
                + "\"ID0\" INTEGER, \"ID1\" INTEGER, "
                + "\"ID2\" INTEGER, \"VAL\" INTEGER, PRIMARY KEY (\"ID0\", \"ID1\", \"ID2\")"
                + ") COLOCATE BY (\"ID0\")"
        );
    }

    @Test
    public void createTableWithIdentifierZone() {
        String sqlQuery = "create table my_table(id int) zone \"zone123\"";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.zone().getSimple(), equalTo("zone123"));

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" (\"ID\" INTEGER) ZONE \"zone123\"");
    }

    @Test
    public void createTableWithMinStaleRows() {
        String sqlQuery = "create table my_table(id int) with (min stale rows 12)";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(
                createTable.tableProperties(),
                hasItem(tablePropertyWithValue(MIN_STALE_ROWS_COUNT, 12))
        );

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" (\"ID\" INTEGER) WITH (MIN STALE ROWS 12)");
    }

    @Test
    public void createTableWithStaleRowsFraction() {
        String sqlQuery = "create table my_table(id int) with (stale rows fraction 0.2)";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(
                createTable.tableProperties(),
                hasItem(tablePropertyWithValue(STALE_ROWS_FRACTION, 0.2))
        );

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" (\"ID\" INTEGER) WITH (STALE ROWS FRACTION 0.2)");
    }

    @Test
    public void createTableWithAllProperties() {
        String sqlQuery = "create table my_table(id int) with (" 
                + "stale rows fraction 0.8," 
                + "min stale rows 500" 
                + ")";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(
                createTable.tableProperties(),
                hasItems(
                        tablePropertyWithValue(STALE_ROWS_FRACTION, 0.8),
                        tablePropertyWithValue(MIN_STALE_ROWS_COUNT, 500)
                )
        );

        expectUnparsed(node, "CREATE TABLE \"MY_TABLE\" (\"ID\" INTEGER) WITH (" 
                + "STALE ROWS FRACTION 0.8," 
                + " MIN STALE ROWS 500" 
                + ")");
    }

    @Test
    public void alterTableWithMinStaleRows() {
        String sqlQuery = "alter table my_table set min stale rows 12";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlAlterTableSetProperties.class));

        IgniteSqlAlterTableSetProperties alterTable = (IgniteSqlAlterTableSetProperties) node;

        assertThat(
                alterTable.propertyList(),
                hasItem(tablePropertyWithValue(MIN_STALE_ROWS_COUNT, 12))
        );

        expectUnparsed(node, "ALTER TABLE \"MY_TABLE\" SET (MIN STALE ROWS 12)");
    }

    @Test
    public void alterTableWithStaleRowsFraction() {
        String sqlQuery = "alter table my_table set stale rows fraction 0.2";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlAlterTableSetProperties.class));

        IgniteSqlAlterTableSetProperties alterTable = (IgniteSqlAlterTableSetProperties) node;

        assertThat(
                alterTable.propertyList(),
                hasItem(tablePropertyWithValue(STALE_ROWS_FRACTION, 0.2))
        );

        expectUnparsed(node, "ALTER TABLE \"MY_TABLE\" SET (STALE ROWS FRACTION 0.2)");
    }

    @Test
    public void alterTableWithAllProperties() {
        String sqlQuery = "alter table my_table set ("
                + "stale rows fraction 0.8,"
                + "min stale rows 500"
                + ")";

        SqlNode node = parse(sqlQuery);

        assertThat(node, instanceOf(IgniteSqlAlterTableSetProperties.class));

        IgniteSqlAlterTableSetProperties alterTable = (IgniteSqlAlterTableSetProperties) node;

        assertThat(
                alterTable.propertyList(),
                hasItems(
                        tablePropertyWithValue(STALE_ROWS_FRACTION, 0.8),
                        tablePropertyWithValue(MIN_STALE_ROWS_COUNT, 500)
                )
        );

        expectUnparsed(node, "ALTER TABLE \"MY_TABLE\" SET ("
                + "STALE ROWS FRACTION 0.8,"
                + " MIN STALE ROWS 500"
                + ")");
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "create table my_table(id int) with (foo rows fraction 0.8);" 
                    + " Failed to parse query: Encountered \"foo\" at line 1, column 37",
            "create table my_table(id int) with (stale row fraction 0.8);"
                    + " Failed to parse query: Encountered \"row\" at line 1, column 43",
            "create table my_table(id int) with (stale rows fraction -0.8);"
                    + " Failed to parse query: Encountered \"-\" at line 1, column 57",
            "create table my_table(id int) with (min stale rows 0.8);"
                    + " Failed to parse query: Encountered \"0.8\" at line 1, column 52",
            "create table my_table(id int) with (min stale rows -500);"
                    + " Failed to parse query: Encountered \"-\" at line 1, column 52",

            "alter table my_table set foo rows fraction 0.8;"
                    + " Failed to parse query: Encountered \"foo\" at line 1, column 26",
            "alter table my_table set stale row fraction 0.8;"
                    + " Failed to parse query: Encountered \"row\" at line 1, column 32",
            "alter table my_table set stale rows fraction -0.8;"
                    + " Failed to parse query: Encountered \"-\" at line 1, column 46",
            "alter table my_table set min stale rows 0.8;"
                    + " Failed to parse query: Encountered \"0.8\" at line 1, column 41",
            "alter table my_table set (min stale rows -500);"
                    + " Failed to parse query: Encountered \"-\" at line 1, column 42",
    })
    public void tablePropertiesParsingErrors(String stmt, String error) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                error,
                () -> parse(stmt));
    }

    @Test
    public void createIndexSimpleCase() {
        var query = "create index my_index on my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.IMPLICIT_SORTED));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL"))));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_TABLE\" (\"COL\")");
    }

    @Test
    public void createIndexImplicitTypeExplicitDirection() {
        var query = "create index my_index on my_table (col1 asc, col2 desc)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.IMPLICIT_SORTED));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col1 asc", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL1"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col2 desc", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.DESCENDING))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("COL2"))));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_TABLE\" (\"COL1\", \"COL2\" DESC)");
    }

    @Test
    public void createIndexExplicitTypeMixedDirection() {
        var query = "create index my_index on my_table using sorted (col1, col2 asc, col3 desc)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.SORTED));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col1", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL1"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col2 asc", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL2"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col3 desc", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.DESCENDING))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("COL3"))));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_TABLE\" USING SORTED (\"COL1\", \"COL2\", \"COL3\" DESC)");
    }

    @Test
    public void createHashIndex() {
        var query = "create index my_index on my_table using hash (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.HASH));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL"))));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_TABLE\" USING HASH (\"COL\")");
    }

    @Test
    public void sortDirectionMustNotBeSpecifiedForHashIndex() {
        var query = "create index my_index on my_table using hash (col1, col2 asc)";

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Encountered \"asc\"",
                () -> parse(query));
    }

    @Test
    public void createIndexIfNotExists() {
        var query = "create index if not exists my_index on my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(true));

        expectUnparsed(node, "CREATE INDEX IF NOT EXISTS \"MY_INDEX\" ON \"MY_TABLE\" (\"COL\")");
    }

    @Test
    public void createIndexTableInParticularSchema() {
        var query = "create index my_index on my_schema.my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_SCHEMA", "MY_TABLE")));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_SCHEMA\".\"MY_TABLE\" (\"COL\")");
    }

    @Test
    public void createIndexExplicitNullDirection() {
        var query = "create index my_index on my_table (col1 nulls first, col2 nulls last, col3 desc nulls first)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col1 nulls first", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.NULLS_FIRST))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("COL1"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col2 nulls last", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.NULLS_LAST))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("COL2"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col3 desc nulls first", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.NULLS_FIRST))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlBasicCall
                        && bc.getOperandList().get(0).isA(Set.of(SqlKind.DESCENDING))
                        && ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().size() == 1
                        && ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) ((SqlBasicCall) bc.getOperandList().get(0)).getOperandList().get(0))
                        .getSimple().equals("COL3"))));

        expectUnparsed(node, "CREATE INDEX \"MY_INDEX\" ON \"MY_TABLE\" ("
                + "\"COL1\" NULLS FIRST, \"COL2\" NULLS LAST, \"COL3\" DESC NULLS FIRST)"
        );
    }

    @Test
    public void dropTable() {
        var query = "drop table my_table";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropTable.class));

        var dropIndex = (IgniteSqlDropTable) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.name().names, is(List.of("MY_TABLE")));

        expectUnparsed(node, "DROP TABLE \"MY_TABLE\"");
    }

    @Test
    public void dropTableSchemaSpecified() {
        var query = "drop table my_schema.my_table";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropTable.class));

        var dropIndex = (IgniteSqlDropTable) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.name().names, is(List.of("MY_SCHEMA", "MY_TABLE")));

        expectUnparsed(node, "DROP TABLE \"MY_SCHEMA\".\"MY_TABLE\"");
    }

    @Test
    public void dropTableIfExists() {
        var query = "drop table if exists my_table";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropTable.class));

        var dropIndex = (IgniteSqlDropTable) node;

        assertThat(dropIndex.ifExists(), is(true));
        assertThat(dropIndex.name().names, is(List.of("MY_TABLE")));

        expectUnparsed(node, "DROP TABLE IF EXISTS \"MY_TABLE\"");
    }

    @Test
    public void dropIndexSimpleCase() {
        var query = "drop index my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.indexName().names, is(List.of("MY_INDEX")));

        expectUnparsed(node, "DROP INDEX \"MY_INDEX\"");
    }

    @Test
    public void dropIndexSchemaSpecified() {
        var query = "drop index my_schema.my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.indexName().names, is(List.of("MY_SCHEMA", "MY_INDEX")));

        expectUnparsed(node, "DROP INDEX \"MY_SCHEMA\".\"MY_INDEX\"");
    }

    @Test
    public void dropIndexIfExists() {
        var query = "drop index if exists my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(true));
        assertThat(dropIndex.indexName().names, is(List.of("MY_INDEX")));

        expectUnparsed(node, "DROP INDEX IF EXISTS \"MY_INDEX\"");
    }

    /**
     * Ensures that the parser throws the expected exception when attempting to use the following unsupported types for table columns.
     *
     * <ol>
     *     <li>{@link SqlTypeName#TIME_TZ TIME WITH TIME ZONE}</li>
     *     <li>{@link SqlTypeName#TIME_WITH_LOCAL_TIME_ZONE TIME WITH LOCAL TIME ZONE}</li>
     *     <li>{@link SqlTypeName#TIMESTAMP_TZ TIMESTAMP WITH TIME ZONE}</li>
     * </ol>
     */
    // TODO: Remove after https://issues.apache.org/jira/browse/IGNITE-21555 is implemented.
    @ParameterizedTest(name = "type={0}")
    @CsvSource({
            "TIME WITH TIME ZONE, Encountered \"WITH\"",
            "TIME WITH LOCAL TIME ZONE, Encountered \"WITH\"",
            "TIMESTAMP WITH TIME ZONE, Encountered \"TIME\""
    })
    public void unsupportedTimeZoneAwareTableColumnTypes(String typeName, String expectedError) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                expectedError,
                () -> parse(format("CREATE TABLE test (ts {})", typeName))
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                expectedError,
                () -> parse(format("ALTER TABLE test ADD COLUMN ts {}", typeName))
        );
    }

    private IgniteSqlCreateTable parseCreateTable(String stmt) {
        SqlNode node = parse(stmt);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        return (IgniteSqlCreateTable) node;
    }

    @Test
    public void alterTableAddColumn() {
        SqlNode sqlNode = parse("ALTER TABLE t ADD COLUMN c INT");

        IgniteSqlAlterTableAddColumn addColumn = assertInstanceOf(IgniteSqlAlterTableAddColumn.class, sqlNode);
        SqlColumnDeclaration declaration = (SqlColumnDeclaration) addColumn.columns().get(0);

        assertThat(addColumn.name.names, is(List.of("T")));
        assertThat(addColumn.ifColumnNotExists(), is(false));

        expectColumnBasic(declaration, "C", ColumnStrategy.NULLABLE, "INTEGER", true);
        assertThat(declaration.expression, is(nullValue()));

        expectUnparsed(addColumn, "ALTER TABLE \"T\" ADD COLUMN \"C\" INTEGER");
    }

    @Test
    public void alterTableAddColumnIfNotExists() {
        SqlNode sqlNode = parse("ALTER TABLE t ADD COLUMN IF NOT EXISTS c INT");

        IgniteSqlAlterTableAddColumn addColumn = assertInstanceOf(IgniteSqlAlterTableAddColumn.class, sqlNode);
        SqlColumnDeclaration declaration = (SqlColumnDeclaration) addColumn.columns().get(0);

        assertThat(addColumn.name.names, is(List.of("T")));
        assertThat(addColumn.ifColumnNotExists(), is(true));

        expectColumnBasic(declaration, "C", ColumnStrategy.NULLABLE, "INTEGER", true);
        assertThat(declaration.expression, is(nullValue()));

        expectUnparsed(addColumn, "ALTER TABLE \"T\" ADD COLUMN IF NOT EXISTS \"C\" INTEGER");
    }

    @Test
    public void alterTableAddColumnNull() {
        SqlNode sqlNode = parse("ALTER TABLE t ADD COLUMN c INT NULL");

        IgniteSqlAlterTableAddColumn addColumn = assertInstanceOf(IgniteSqlAlterTableAddColumn.class, sqlNode);
        SqlColumnDeclaration column = (SqlColumnDeclaration) addColumn.columns().get(0);

        assertThat(addColumn.name.names, is(List.of("T")));

        expectColumnBasic(column, "C", ColumnStrategy.NULLABLE, "INTEGER", true);
        assertThat(column.expression, is(nullValue()));

        expectUnparsed(addColumn, "ALTER TABLE \"T\" ADD COLUMN \"C\" INTEGER");
    }

    @Test
    public void alterTableAddColumnNotNull() {
        SqlNode sqlNode = parse("ALTER TABLE t ADD COLUMN c INT NOT NULL");

        IgniteSqlAlterTableAddColumn addColumn = assertInstanceOf(IgniteSqlAlterTableAddColumn.class, sqlNode);
        SqlColumnDeclaration column = (SqlColumnDeclaration) addColumn.columns().get(0);

        assertThat(addColumn.name.names, is(List.of("T")));

        expectColumnBasic(column, "C", ColumnStrategy.NOT_NULLABLE, "INTEGER", false);
        assertThat(column.expression, is(nullValue()));

        expectUnparsed(addColumn, "ALTER TABLE \"T\" ADD COLUMN \"C\" INTEGER NOT NULL");
    }

    @Test
    public void alterTableDropColumn() {
        SqlNode sqlNode = parse("ALTER TABLE t DROP COLUMN c");

        IgniteSqlAlterTableDropColumn dropColumn = assertInstanceOf(IgniteSqlAlterTableDropColumn.class, sqlNode);

        assertThat(dropColumn.name.names, is(List.of("T")));
        assertThat(dropColumn.ifColumnExists(), is(false));

        expectUnparsed(dropColumn, "ALTER TABLE \"T\" DROP COLUMN \"C\"");
    }

    @Test
    public void alterTableDropColumnIfExists() {
        SqlNode sqlNode = parse("ALTER TABLE t DROP COLUMN IF EXISTS c");

        IgniteSqlAlterTableDropColumn dropColumn = assertInstanceOf(IgniteSqlAlterTableDropColumn.class, sqlNode);

        assertThat(dropColumn.name.names, is(List.of("T")));
        assertThat(dropColumn.ifColumnExists(), is(true));

        expectUnparsed(dropColumn, "ALTER TABLE \"T\" DROP COLUMN IF EXISTS \"C\"");
    }

    /**
     * CHAR datatype has certain storage assignment rules, namely it requires value to be padded with `space` character up to declared
     * length. This currently not supported in storage, thus let's forbid usage of CHAR datatype for table's columns.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c CHAR)",
            "CREATE TABLE t (c CHAR(5))",
            "CREATE TABLE t (c CHARACTER)",
            "CREATE TABLE t (c CHARACTER(5))",
            "ALTER TABLE t ADD COLUMN c CHAR",
            "ALTER TABLE t ADD COLUMN c CHAR(5)",
            "ALTER TABLE t ADD COLUMN c CHARACTER",
            "ALTER TABLE t ADD COLUMN c CHARACTER(5)",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE CHAR",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE CHAR(5)",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE CHARACTER",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE CHARACTER(5)"
    })
    void charTypeIsNotAllowedInTable(String statement) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "CHAR datatype is not supported in table",
                () -> parse(statement)
        );
    }

    /**
     * Test makes sure exception is not thrown for CHARACTER VARYING type and in CAST operation where CHARACTER is allowed.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c VARCHAR)",
            "CREATE TABLE t (c CHARACTER VARYING)",
            "SELECT CAST(1 AS CHAR)",
            "SELECT CAST(1 AS CHARACTER)",
            "SELECT 1::CHAR",
            "SELECT 1::CHARACTER",
    })
    void restrictionOfCharTypeIsNotAppliedToVarcharAndCast(String statement) {
        SqlNode node = parse(statement);

        assertNotNull(node);
    }

    /**
     * BINARY datatype has certain storage assignment rules, namely it requires value to be padded with X'00' symbol up to declared
     * length. This currently not supported in storage, thus let's forbid usage of BINARY datatype for table's columns.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c BINARY)",
            "CREATE TABLE t (c BINARY(5))",
            "ALTER TABLE t ADD COLUMN c BINARY",
            "ALTER TABLE t ADD COLUMN c BINARY(5)",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE BINARY",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE BINARY(5)",
    })
    void binaryTypeIsNotAllowedInTable(String statement) {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "BINARY datatype is not supported in table",
                () -> parse(statement)
        );
    }

    /**
     * Test makes sure exception is not thrown for BINARY VARYING type and in CAST operation where BINARY is allowed.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE t (c VARBINARY)",
            "CREATE TABLE t (c BINARY VARYING)",
            "SELECT CAST(1 AS BINARY)",
            "SELECT 1::BINARY",
    })
    void restrictionOfBinaryTypeIsNotAppliedToVarbinaryAndCast(String statement) {
        SqlNode node = parse(statement);

        assertNotNull(node);
    }

    /**
     * Matcher to verify name in the column declaration.
     *
     * @param name Expected name.
     * @return {@code true} in case name in the column declaration equals to the expected one.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> columnWithName(String name) {
        return new CustomMatcher<>("column with name=" + name) {
            /** {@inheritDoc} */
            @Override
            public boolean matches(Object item) {
                return item instanceof SqlColumnDeclaration
                        && ((SqlColumnDeclaration) item).name.names.get(0).equals(name);
            }
        };
    }

    /** Checks basic column properties such as name, type name and type's nullability. */
    private static void expectColumnBasic(SqlColumnDeclaration declaration,
            String columnName, ColumnStrategy columnStrategy,
            String typeName, Boolean nullable) {

        assertThat(List.of(columnName), is(declaration.name.names));
        assertThat(columnStrategy, is(declaration.strategy));
        assertThat(List.of(typeName), is(declaration.dataType.getTypeName().names));
        assertThat(nullable, is(declaration.dataType.getNullable()));
    }

    private static Matcher<SqlNode> tablePropertyWithValue(IgniteSqlTablePropertyKey property, Object value) {
        return ofTypeMatching(
                property + "=" + value,
                IgniteSqlTableProperty.class,
                opt -> {
                    if (property == opt.key()) {
                        if (opt.value() instanceof SqlLiteral) {
                            return Objects.equals(value, ((SqlLiteral) opt.value()).getValueAs(value.getClass()));
                        }
                    }

                    return false;
                }
        );
    }
}
