/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the DDL command.
 */
public class SqlDdlParserTest {
    /**
     * Very simple case where only table name and a few columns are presented.
     */
    @Test
    public void createTableSimpleCase() throws SqlParseException {
        String query = "create table my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));
    }

    /**
     * Parsing of CREATE TABLE with function identifier as a default expression.
     */
    @Test
    public void createTableAutogenFuncDefault() throws SqlParseException {
        String query = "create table my_table(id varchar default gen_random_uuid primary key, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "Column with function's identifier as default",
                SqlColumnDeclaration.class,
                col -> "ID".equals(col.name.getSimple())
                        && col.expression instanceof SqlIdentifier
                        && "GEN_RANDOM_UUID".equals(((SqlIdentifier) col.expression).getSimple())
        )));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE statement with quoted identifiers.
     */
    @Test
    public void createTableQuotedIdentifiers() throws SqlParseException {
        String query = "create table \"My_Table\"(\"Id\" int, \"Val\" varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("My_Table")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(columnWithName("Id")));
        assertThat(createTable.columnList(), hasItem(columnWithName("Val")));
    }

    /**
     * Parsing of CREATE TABLE statement with IF NOT EXISTS.
     */
    @Test
    public void createTableIfNotExists() throws SqlParseException {
        String query = "create table if not exists my_table(id int, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(true));
        assertThat(createTable.columnList(), hasItem(columnWithName("ID")));
        assertThat(createTable.columnList(), hasItem(columnWithName("VAL")));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is a shortcut within a column definition.
     */
    @Test
    public void createTableWithPkCase1() throws SqlParseException {
        String query = "create table my_table(id int primary key, val varchar)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has no name.
     */
    @Test
    public void createTableWithPkCase2() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint without name containing column \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint is set explicitly and has a name.
     */
    @Test
    public void createTableWithPkCase3() throws SqlParseException {
        String query = "create table my_table(id int, val varchar, constraint pk_key primary key(id))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with name \"PK_KEY\" containing column \"ID\"", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID\"", SqlIdentifier.class, id -> "ID".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && "PK_KEY".equals(((SqlIdentifier) constraint.getOperandList().get(0)).names.get(0))
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified PK constraint where constraint consists of several columns.
     */
    @Test
    public void createTableWithPkCase4() throws SqlParseException {
        String query = "create table my_table(id1 int, id2 int, val varchar, primary key(id1, id2))";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.ifNotExists, is(false));
        assertThat(createTable.columnList(), hasItem(ofTypeMatching(
                "PK constraint with two columns", SqlKeyConstraint.class,
                constraint -> hasItem(ofTypeMatching("identifier \"ID1\"", SqlIdentifier.class, id -> "ID1".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && hasItem(ofTypeMatching("identifier \"ID2\"", SqlIdentifier.class, id -> "ID2".equals(id.names.get(0))))
                        .matches(constraint.getOperandList().get(1))
                        && constraint.getOperandList().get(0) == null
                        && constraint.isA(singleton(SqlKind.PRIMARY_KEY)))));
    }

    /**
     * Parsing of CREATE TABLE with specified colocation columns.
     */
    @Test
    public void createTableWithColocationBy() throws SqlParseException {
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

        SqlPrettyWriter w = new SqlPrettyWriter();
        createTable.unparse(w, 0, 0);

        assertThat(w.toString(), endsWith("COLOCATE BY (\"ID2\", \"ID1\")"));

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
                "CREATE TABLE MY_TABLE(ID0 INT, ID1 INT, ID2 INT, VAL INT, PRIMARY KEY (ID0, ID1, ID2)) COLOCATE (ID0) "
                        + "with "
                        + "replicas=2, "
                        + "partitions=3"
        );

        w = new SqlPrettyWriter();
        createTable.unparse(w, 0, 0);

        assertThat(w.toString(), endsWith("COLOCATE BY (\"ID0\") WITH \"REPLICAS\" = 2, \"PARTITIONS\" = 3"));
    }

    @Test
    public void createTableWithEngine() throws SqlParseException {
        SqlNode node = parse("create table my_table(id int, val varchar) engine test_engine_name");

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.engineName().names, is(List.of("TEST_ENGINE_NAME")));
    }

    @Test
    public void createTableWithoutEngine() throws SqlParseException {
        SqlNode node = parse("create table my_table(id int, val varchar)");

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThat(createTable.name().names, is(List.of("MY_TABLE")));
        assertThat(createTable.engineName(), nullValue());
    }

    @Test
    public void createTableWithOptions() throws SqlParseException {
        String query = "create table my_table(id int) with"
                + " replicas=2,"
                + " partitions=3,"
                + " dataRegion='default',"
                + " \"pageSize\"=1024,"
                + " \"persistent\"=true";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        IgniteSqlCreateTable createTable = (IgniteSqlCreateTable) node;

        assertThatOptionPresent(createTable.createOptionList().getList(), "REPLICAS", 2);
        assertThatOptionPresent(createTable.createOptionList().getList(), "PARTITIONS", 3);
        assertThatOptionPresent(createTable.createOptionList().getList(), "DATAREGION", "default");
        assertThatOptionPresent(createTable.createOptionList().getList(), "pageSize", 1024);
        assertThatOptionPresent(createTable.createOptionList().getList(), "persistent", true);
    }

    @Test
    public void createIndexSimpleCase() throws SqlParseException {
        var query = "create index my_index on my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.IMPLICIT_TREE));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL"))));
    }

    @Test
    public void createIndexImplicitTypeExplicitDirection() throws SqlParseException {
        var query = "create index my_index on my_table (col1 asc, col2 desc)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.IMPLICIT_TREE));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col1 asc", SqlIdentifier.class,
                id -> id.isSimple() && id.getSimple().equals("COL1"))));
        assertThat(createIndex.columnList(), hasItem(ofTypeMatching("col2 desc", SqlBasicCall.class,
                bc -> bc.isA(Set.of(SqlKind.DESCENDING))
                        && bc.getOperandList().size() == 1
                        && bc.getOperandList().get(0) instanceof SqlIdentifier
                        && ((SqlIdentifier) bc.getOperandList().get(0)).isSimple()
                        && ((SqlIdentifier) bc.getOperandList().get(0)).getSimple().equals("COL2"))));
    }

    @Test
    public void createIndexExplicitTypeMixedDirection() throws SqlParseException {
        var query = "create index my_index on my_table using tree (col1, col2 asc, col3 desc)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(false));
        assertThat(createIndex.type(), is(IgniteSqlIndexType.TREE));
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
    }

    @Test
    public void createHashIndex() throws SqlParseException {
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
    }

    @Test
    public void sortDirectionMustNotBeSpecifiedForHashIndex() {
        var query = "create index my_index on my_table using hash (col1, col2 asc)";

        var ex = assertThrows(SqlParseException.class, () -> parse(query));
        assertThat(ex.getMessage(), containsString("Encountered \" \"ASC\""));
    }

    @Test
    public void createIndexIfNotExists() throws SqlParseException {
        var query = "create index if not exists my_index on my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_TABLE")));
        assertThat(createIndex.ifNotExists, is(true));
    }

    @Test
    public void createIndexTableInParticularSchema() throws SqlParseException {
        var query = "create index my_index on my_schema.my_table (col)";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlCreateIndex.class));

        var createIndex = (IgniteSqlCreateIndex) node;

        assertThat(createIndex.indexName().getSimple(), is("MY_INDEX"));
        assertThat(createIndex.tableName().names, is(List.of("MY_SCHEMA", "MY_TABLE")));
    }

    @Test
    public void createIndexExplicitNullDirection() throws SqlParseException {
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
    }

    @Test
    public void dropIndexSimpleCase() throws SqlParseException {
        var query = "drop index my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.indexName().names, is(List.of("MY_INDEX")));
    }

    @Test
    public void dropIndexSchemaSpecified() throws SqlParseException {
        var query = "drop index my_schema.my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(false));
        assertThat(dropIndex.indexName().names, is(List.of("MY_SCHEMA", "MY_INDEX")));
    }

    @Test
    public void dropIndexIfExists() throws SqlParseException {
        var query = "drop index if exists my_index";

        SqlNode node = parse(query);

        assertThat(node, instanceOf(IgniteSqlDropIndex.class));

        var dropIndex = (IgniteSqlDropIndex) node;

        assertThat(dropIndex.ifExists(), is(true));
        assertThat(dropIndex.indexName().names, is(List.of("MY_INDEX")));
    }

    private IgniteSqlCreateTable parseCreateTable(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);

        assertThat(node, instanceOf(IgniteSqlCreateTable.class));

        return (IgniteSqlCreateTable) node;
    }

    /**
     * Parses a given statement and returns a resulting AST.
     *
     * @param stmt Statement to parse.
     * @return An AST.
     */
    private static SqlNode parse(String stmt) throws SqlParseException {
        SqlParser parser = SqlParser.create(stmt, SqlParser.config().withParserFactory(IgniteSqlParserImpl.FACTORY));

        return parser.parseStmt();
    }

    /**
     * Matcher to verify name in the column declaration.
     *
     * @param name Expected name.
     * @return {@code true} in case name in the column declaration equals to the expected one.
     */
    private static <T extends SqlColumnDeclaration> Matcher<T> columnWithName(String name) {
        return new CustomMatcher<T>("column with name=" + name) {
            /** {@inheritDoc} */
            @Override
            public boolean matches(Object item) {
                return item instanceof SqlColumnDeclaration
                        && ((SqlColumnDeclaration) item).name.names.get(0).equals(name);
            }
        };
    }

    /**
     * Matcher to verify that an object of the expected type and matches the given predicate.
     *
     * @param desc Description for this matcher.
     * @param cls  Expected class to verify the object is instance of.
     * @param pred Addition check that would be applied to the object.
     * @return {@code true} in case the object if instance of the given class and matches the predicat.
     */
    private static <T> Matcher<T> ofTypeMatching(String desc, Class<T> cls, Predicate<T> pred) {
        return new CustomMatcher<T>(desc) {
            /** {@inheritDoc} */
            @Override
            public boolean matches(Object item) {
                return item != null && cls.isAssignableFrom(item.getClass()) && pred.test((T) item);
            }
        };
    }

    private static void assertThatOptionPresent(List<SqlNode> optionList, String option, Object expVal) {
        assertThat(optionList, hasItem(ofTypeMatching(
                option + "=" + expVal,
                IgniteSqlCreateTableOption.class,
                opt -> {
                    if (opt.key().getSimple().equals(option)) {
                        if (opt.value() instanceof SqlLiteral) {
                            return Objects.equals(expVal, ((SqlLiteral) opt.value()).getValueAs(expVal.getClass()));
                        }
                    }

                    return false;
                }
        )));
    }
}
