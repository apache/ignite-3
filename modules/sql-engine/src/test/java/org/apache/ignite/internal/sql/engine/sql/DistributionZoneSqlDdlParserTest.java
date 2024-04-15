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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the DDL "ZONE" commands.
 */
public class DistributionZoneSqlDdlParserTest extends AbstractDdlParserTest {

    /**
     * Parse simple CREATE ZONE statement.
     */
    @Test
    public void createZoneNoOptions() {
        // Simple name.
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone");

        assertThat(createZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(createZone.ifNotExists());
        assertNull(createZone.createOptionList());
        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\"");

        // Fully qualified name.
        createZone = parseCreateZone("create zone public.test_zone");
        assertThat(createZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));
        expectUnparsed(createZone, "CREATE ZONE \"PUBLIC\".\"TEST_ZONE\"");

        // Quoted identifier.
        createZone = parseCreateZone("create zone \"public\".\"test_Zone\"");
        assertThat(createZone.name().names, is(List.of("public", "test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public\".\"test_Zone\"");

        createZone = parseCreateZone("create zone \"public-test_Zone\"");
        assertThat(createZone.name().names, is(List.of("public-test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public-test_Zone\"");
    }

    /**
     * Parse CREATE ZONE IF NOT EXISTS statement.
     */
    @Test
    public void createZoneIfNotExists() {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone if not exists test_zone");

        assertTrue(createZone.ifNotExists());
        assertNull(createZone.createOptionList());

        expectUnparsed(createZone, "CREATE ZONE IF NOT EXISTS \"TEST_ZONE\"");
    }

    /**
     * Parse CREATE ZONE WITH ... statement.
     */
    @Test
    public void createZoneWithOptions() {
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

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.PARTITIONS, 3);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.AFFINITY_FUNCTION, "test_Affinity");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST, 1);

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" WITH "
                + "\"REPLICAS\" = 2, "
                + "\"PARTITIONS\" = 3, "
                + "\"DATA_NODES_FILTER\" = '(\"US\" || \"EU\") && \"SSD\"', "
                + "\"AFFINITY_FUNCTION\" = 'test_Affinity', "
                + "\"DATA_NODES_AUTO_ADJUST\" = 1, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 2, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 3");
    }

    /**
     * Parsing DROP ZONE statement.
     */
    @Test
    public void dropZone() {
        // Simple name.
        SqlNode node = parse("drop zone test_zone");

        assertThat(node, instanceOf(IgniteSqlDropZone.class));

        IgniteSqlDropZone dropZone = (IgniteSqlDropZone) node;

        assertThat(dropZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(dropZone.ifExists());

        expectUnparsed(node, "DROP ZONE \"TEST_ZONE\"");

        // Fully qualified name.
        dropZone = ((IgniteSqlDropZone) parse("drop zone public.test_zone"));

        assertThat(dropZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));
        expectUnparsed(dropZone, "DROP ZONE \"PUBLIC\".\"TEST_ZONE\"");
    }

    /**
     * Parsing DROP ZONE IF EXISTS statement.
     */
    @Test
    public void dropZoneIfExists() {
        IgniteSqlDropZone dropZone = (IgniteSqlDropZone) parse("drop zone if exists test_zone");

        assertTrue(dropZone.ifExists());

        expectUnparsed(dropZone, "DROP ZONE IF EXISTS \"TEST_ZONE\"");
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement.
     */
    @Test
    public void alterZoneRenameTo() {
        IgniteSqlAlterZoneRenameTo alterZone = parseAlterZoneRenameTo("alter zone a.test_zone rename to zone1");
        assertFalse(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" RENAME TO \"ZONE1\"";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement.
     */
    @Test
    public void alterZoneIfExistsRenameTo() {
        IgniteSqlAlterZoneRenameTo alterZone = parseAlterZoneRenameTo("alter zone if exists a.test_zone rename to zone1");
        assertTrue(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" RENAME TO \"ZONE1\"";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE SET DEFAULT statement.
     */
    @Test
    public void alterZoneSetDefault() {
        IgniteSqlAlterZoneSetDefault alterZone =
                assertInstanceOf(IgniteSqlAlterZoneSetDefault.class, parse("alter zone a.test_zone set default"));

        assertFalse(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET DEFAULT";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE IF EXISTS SET DEFAULT statement.
     */
    @Test
    public void alterZoneIfExistsSetDefault() {
        IgniteSqlAlterZoneSetDefault alterZone =
                assertInstanceOf(IgniteSqlAlterZoneSetDefault.class, parse("alter zone if exists a.test_zone set default"));
        assertTrue(alterZone.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" SET DEFAULT";
        expectUnparsed(alterZone, expectedStmt);
    }

    /**
     * Ensures that we cannot change zone parameters and set this zone as default in the same request.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void alterZoneSetDefaultWithOptionsIsIllegal() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set replicas=2, default")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set default, replicas=2")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set default replicas=2")
        );
    }

    /**
     * Parsing ALTER ZONE RENAME TO statement with invalid arguments.
     */
    @Test
    public void alterZoneRenameToWithCompoundIdIsIllegal() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query", () -> parse("alter zone a.test_zone rename to b.zone1"));
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSet() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone a.test_zone set replicas=2");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET \"REPLICAS\" = 2";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE IF EXISTS SET statement.
     */
    @Test
    public void alterZoneIfExistsSet() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone if exists a.test_zone set replicas=2");
        assertTrue(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" SET \"REPLICAS\" = 2";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSetOptions() {
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

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST, 1);

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET "
                + "\"REPLICAS\" = 2, "
                + "\"DATA_NODES_FILTER\" = '(\"US\" || \"EU\") && \"SSD\"', "
                + "\"DATA_NODES_AUTO_ADJUST\" = 1, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_UP\" = 2, "
                + "\"DATA_NODES_AUTO_ADJUST_SCALE_DOWN\" = 3";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parses ALTER ZONE SET w/o provided options.
     */
    @Test
    public void alterZoneSetNoOptionsIsIllegal() {
        assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query", () -> parse("alter zone test_zone set"));
    }

    /**
     * Parse CREATE ZONE statement.
     *
     * @param stmt Create zone query.
     * @return {@link org.apache.calcite.sql.SqlCreate SqlCreate} node.
     */
    private IgniteSqlCreateZone parseCreateZone(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlCreateZone.class, node);
    }

    /**
     * Parse ALTER ZONE SET statement.
     */
    private IgniteSqlAlterZoneSet parseAlterZoneSet(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneSet.class, node);
    }

    /**
     * Parse ALTER ZONE RENAME TO statement.
     */
    private IgniteSqlAlterZoneRenameTo parseAlterZoneRenameTo(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneRenameTo.class, node);
    }

    private void assertThatZoneOptionPresent(List<SqlNode> optionList, ZoneOptionEnum name, Object expVal) {
        assertThat(optionList, Matchers.hasItem(ofTypeMatching(
                name + "=" + expVal,
                IgniteSqlZoneOption.class,
                opt -> {
                    if (name.name().equals(opt.key().getSimple())) {
                        if (opt.value() instanceof SqlLiteral) {
                            return Objects.equals(expVal, ((SqlLiteral) opt.value()).getValueAs(expVal.getClass()));
                        }
                    }

                    return false;
                }
        )));
    }
}
