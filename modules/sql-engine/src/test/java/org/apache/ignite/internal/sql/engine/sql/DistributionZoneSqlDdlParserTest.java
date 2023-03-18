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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.sql.SqlException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the DDL "ZONE" commands.
 */
public class DistributionZoneSqlDdlParserTest extends AbstractDdlParserTest {

    private static final List<String> NUMERIC_OPTIONS = Arrays.asList("PARTITIONS", "REPLICAS", "DATA_NODES_AUTO_ADJUST",
            "DATA_NODES_AUTO_ADJUST_SCALE_UP", "DATA_NODES_AUTO_ADJUST_SCALE_DOWN");
    private static final List<String> STRING_OPTIONS = Arrays.asList("AFFINITY_FUNCTION", "DATA_NODES_FILTER");

    /**
     * Parse simple CREATE ZONE statement.
     */
    @Test
    public void createZoneNoOptions() throws SqlParseException {
        // Simple name.
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone");

        assertThat(createZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(createZone.ifNotExists());
        assertNull(createZone.createOptionList());

        // Fully qualified name.
        createZone = parseCreateZone("create zone public.test_zone");
        assertThat(createZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));

        // Quoted identifier.
        createZone = parseCreateZone("create zone \"public\".\"test_Zone\"");
        assertThat(createZone.name().names, is(List.of("public", "test_Zone")));

        createZone = parseCreateZone("create zone \"public-test_Zone\"");
        assertThat(createZone.name().names, is(List.of("public-test_Zone")));
    }

    /**
     * Parse CREATE ZONE IF NOT EXISTS statement.
     */
    @Test
    public void createZoneIfNotExists() throws SqlParseException {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone if not exists test_zone");

        assertTrue(createZone.ifNotExists());
        assertNull(createZone.createOptionList());
    }

    /**
     * Parse CREATE ZONE WITH ... statement.
     */
    @Test
    public void createZoneWithOptions() throws SqlParseException {
        IgniteSqlCreateZone createZone = parseCreateZone(
                "create zone test_zone with "
                        + "replicas=2, "
                        + "partitions=3, "
                        + "data_nodes_filter='(\"US\" || \"EU\") && \"SSD\"', "
                        + "affinity_function='test_Affinity', "
                        + "data_nodes_auto_adjust=1, "
                        + "data_nodes_auto_adjust_scale_up=2, "
                        + "data_nodes_auto_adjust_scale_down=3"
        );

        assertNotNull(createZone.createOptionList());

        List<SqlNode> optList = createZone.createOptionList().getList();

        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.PARTITIONS, 3);
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.AFFINITY_FUNCTION, "test_Affinity");
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.DATA_NODES_AUTO_ADJUST, 1);

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "REPLICAS = 2, "
                + "PARTITIONS = 3, "
                + "DATA_NODES_FILTER = '(\"US\" || \"EU\") && \"SSD\"', "
                + "AFFINITY_FUNCTION = 'test_Affinity', "
                + "DATA_NODES_AUTO_ADJUST = 1, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_UP = 2, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_DOWN = 3");
    }

    /**
     * Parse CREATE ZONE WITH unknown option.
     */
    @Test
    public void createZoneWithInvalidOptions() {
        // Unknown option.
        assertThrows(SqlException.class, () -> parseCreateZone("create zone test_zone with foo='bar'"));

        // Invalid option type.
        String query = "create zone test_zone with %s=%s";

        for (String optName : NUMERIC_OPTIONS) {
            assertSqlParseError(String.format(query, optName, "'bar'"), optName);
        }

        for (String optName : STRING_OPTIONS) {
            assertSqlParseError(String.format(query, optName, "1"), optName);
        }
    }

    /**
     * Parsing DROP ZONE statement.
     */
    @Test
    public void dropZone() throws SqlParseException {
        // Simple name.
        SqlNode node = parse("drop zone test_zone");

        assertThat(node, instanceOf(IgniteSqlDropZone.class));

        IgniteSqlDropZone dropZone = (IgniteSqlDropZone) node;

        assertThat(dropZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(dropZone.ifExists());

        // Fully qualified name.
        dropZone = ((IgniteSqlDropZone) parse("drop zone public.test_zone"));

        assertThat(dropZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));
    }

    /**
     * Parsing DROP ZONE IF EXISTS statement.
     */
    @Test
    public void dropZoneIfExists() throws SqlParseException {
        IgniteSqlDropZone dropZone = (IgniteSqlDropZone) parse("drop zone if exists test_zone");

        assertTrue(dropZone.ifExists());

        expectUnparsed(dropZone, "DROP ZONE IF EXISTS \"TEST_ZONE\"");
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement.
     */
    @Test
    public void alterZoneRenameTo() throws SqlParseException {
        IgniteSqlAlterZoneRenameTo alterZone = parseAlterZoneRenameTo("alter zone a.test_zone rename to zone1");
        assertFalse(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" RENAME TO \"ZONE1\"";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement.
     */
    @Test
    public void alterZoneIfExistsRenameTo() throws SqlParseException {
        IgniteSqlAlterZoneRenameTo alterZone = parseAlterZoneRenameTo("alter zone if exists a.test_zone rename to zone1");
        assertTrue(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" RENAME TO \"ZONE1\"";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement with invalid arguments.
     */
    @Test
    public void alterZoneRenameToWithCompoundIdIsIllegal() {
        assertThrows(SqlException.class, () -> parse("alter zone a.test_zone rename to b.zone1"));
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSet() throws SqlParseException {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone a.test_zone set replicas=2");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET REPLICAS = 2";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE IF EXISTS SET statement.
     */
    @Test
    public void alterZoneIfExistsSet() throws SqlParseException {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone if exists a.test_zone set replicas=2");
        assertTrue(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" SET REPLICAS = 2";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSetOptions() throws SqlParseException {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet(
                "alter zone a.test_zone set "
                        + "replicas=2, "
                        + "data_nodes_filter='(\"US\" || \"EU\") && \"SSD\"', "
                        + "data_nodes_auto_adjust=1, "
                        + "data_nodes_auto_adjust_scale_up=2, "
                        + "data_nodes_auto_adjust_scale_down=3"
        );

        assertEquals(List.of("A", "TEST_ZONE"), alterZoneSet.name().names);
        assertNotNull(alterZoneSet.alterOptionsList());
        assertFalse(alterZoneSet.ifExists());

        List<SqlNode> optList = alterZoneSet.alterOptionsList().getList();

        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, IgniteSqlZoneOptionEnum.DATA_NODES_AUTO_ADJUST, 1);

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET "
                + "REPLICAS = 2, "
                + "DATA_NODES_FILTER = '(\"US\" || \"EU\") && \"SSD\"', "
                + "DATA_NODES_AUTO_ADJUST = 1, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_UP = 2, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_DOWN = 3";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parses ALTER ZONE SET w/o provided options.
     */
    @Test
    public void alterZoneSetNoOptionsIsIllegal() {
        assertThrows(SqlException.class, () -> parse("alter zone test_zone set"));
    }

    /**
     * Parses ALTER ZONE WITH invalid options.
     */
    @Test
    public void alterZoneSetInvalidOptions() {
        String query = "alter zone test_zone set %s=%s";

        // invalid option

        assertThrows(SqlException.class, () -> parse(String.format(query, "foo", "'bar'")));

        // invalid option values

        for (String optName : NUMERIC_OPTIONS) {
            assertSqlParseError(String.format(query, optName, "'bar'"), optName);
        }

        for (String optName : STRING_OPTIONS) {
            assertSqlParseError(String.format(query, optName, "1"), optName);
        }

        // non modifiable options can only be set in CREATE ZONE.

        assertSqlParseError(String.format(query, "PARTITIONS", 2), "partitions");
        assertSqlParseError(String.format(query, "AFFINITY_FUNCTION", "'function'"), "affinity_function");
    }

    /**
     * Parse CREATE ZONE statement.
     *
     * @param stmt Create zone query.
     * @return {@link org.apache.calcite.sql.SqlCreate SqlCreate} node.
     */
    private IgniteSqlCreateZone parseCreateZone(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlCreateZone.class, node);
    }

    /**
     * Parse ALTER ZONE SET statement.
     */
    private IgniteSqlAlterZoneSet parseAlterZoneSet(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneSet.class, node);
    }

    /**
     * Parse ALTER ZONE RENAME TO statement.
     */
    private IgniteSqlAlterZoneRenameTo parseAlterZoneRenameTo(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneRenameTo.class, node);
    }

    /**
     * Compares the result of calling {@link SqlNode#unparse(SqlWriter, int, int)}} on the given node with the expected string.
     */
    private static void expectUnparsed(SqlNode node, String expectedStmt) {
        SqlPrettyWriter w = new SqlPrettyWriter();
        node.unparse(w, 0, 0);

        assertThat(w.toString(), equalTo(expectedStmt));
    }

    private void assertSqlParseError(String stmt, String name) {
        assertThrows(SqlException.class, () -> parse(stmt), name);
    }

    private void assertThatZoneOptionPresent(List<SqlNode> optionList, IgniteSqlZoneOptionEnum name, Object expVal) {
        assertThat(optionList, Matchers.hasItem(ofTypeMatching(
                name + "=" + expVal,
                IgniteSqlZoneOption.class,
                opt -> {
                    if (name == opt.key().symbolValue(IgniteSqlZoneOptionEnum.class)) {
                        if (opt.value() instanceof SqlLiteral) {
                            return Objects.equals(expVal, ((SqlLiteral) opt.value()).getValueAs(expVal.getClass()));
                        }
                    }

                    return false;
                }
        )));
    }
}
