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
public class DistributionZoneSqlDdlParserTest extends AbstractParserTest {

    /**
     * Parse simple CREATE ZONE statement.
     */
    @Test
    public void createZoneNoOptions() {
        // Simple name.
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone storage profiles['p']");

        assertThat(createZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(createZone.ifNotExists());
        assertNotNull(createZone.createOptionList());
        assertTrue(createZone.createOptionList().isEmpty());
        assertNotNull(createZone.storageProfiles());
        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" STORAGE PROFILES['p']");

        // Fully qualified name.
        createZone = parseCreateZone("create zone public.test_zone storage profiles['p']");
        assertThat(createZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));
        expectUnparsed(createZone, "CREATE ZONE \"PUBLIC\".\"TEST_ZONE\" STORAGE PROFILES['p']");

        // Quoted identifier.
        createZone = parseCreateZone("create zone \"public\".\"test_Zone\" storage profiles['p']");
        assertThat(createZone.name().names, is(List.of("public", "test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public\".\"test_Zone\" STORAGE PROFILES['p']");

        createZone = parseCreateZone("create zone \"public-test_Zone\" storage profiles['p']");
        assertThat(createZone.name().names, is(List.of("public-test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public-test_Zone\" STORAGE PROFILES['p']");
    }

    /**
     * Parse CREATE ZONE IF NOT EXISTS statement.
     */
    @Test
    public void createZoneIfNotExists() {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone if not exists test_zone storage profiles['p']");

        assertTrue(createZone.ifNotExists());
        assertNotNull(createZone.createOptionList());
        assertTrue(createZone.createOptionList().isEmpty());
        assertNotNull(createZone.storageProfiles());

        expectUnparsed(createZone, "CREATE ZONE IF NOT EXISTS \"TEST_ZONE\" STORAGE PROFILES['p']");
    }

    /** Parse CREATE ZONE ... statement. */
    @Test
    public void createZoneWithOptions() {
        IgniteSqlCreateZone createZone = parseCreateZone(
                "create zone test_zone "
                        + "(replicas 2, "
                        + "quorum size 2, "
                        + "partitions 3, "
                        + "nodes filter '(\"US\" || \"EU\") && \"SSD\"', "
                        + "distribution algorithm 'test_Distribution', "
                        + "auto scale up 2, "
                        + "auto scale down 3, "
                        + "consistency mode 'HIGH_AVAILABILITY') "
                        + "storage profiles ['default', 'new']"
        );

        assertNotNull(createZone.createOptionList());

        List<SqlNode> optList = createZone.createOptionList().getList();

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.QUORUM_SIZE, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.PARTITIONS, 3);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DISTRIBUTION_ALGORITHM, "test_Distribution");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN, 3);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.CONSISTENCY_MODE, "HIGH_AVAILABILITY");

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" ("
                + "REPLICAS 2, "
                + "QUORUM SIZE 2, "
                + "PARTITIONS 3, "
                + "NODES FILTER '(\"US\" || \"EU\") && \"SSD\"', "
                + "DISTRIBUTION ALGORITHM 'test_Distribution', "
                + "AUTO SCALE UP 2, "
                + "AUTO SCALE DOWN 3, "
                + "CONSISTENCY MODE 'HIGH_AVAILABILITY') "
                + "STORAGE PROFILES['default', 'new']");
    }

    /**
     * Parse CREATE ZONE WITH ... statement.
     */
    @Test
    public void createZoneWithAllReplicas() {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone (replicas ALL) STORAGE PROFILES['p']");

        assertNotNull(createZone.createOptionList());

        List<SqlNode> optList = createZone.createOptionList().getList();

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, IgniteSqlZoneOptionMode.ALL);

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" (REPLICAS ALL) STORAGE PROFILES['p']");
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
    public void alterZoneSetDefaultWithOptionsIsIllegal() {
        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set (replicas 2, default)")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set (default, replicas 2)")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set (default replicas 2)")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set replicas 2 partitions 2")
        );

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query",
                () -> parse("alter zone a.test_zone set replicas 2, partitions 2")
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
    public void alterZoneSetReplicas() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZone("alter zone a.test_zone set (replicas 2)");
        IgniteSqlAlterZoneSet alterZoneSetOneParam = parseAlterZone("alter zone a.test_zone set replicas 2");
        IgniteSqlAlterZoneSet alterZoneSetOneEqParam = parseAlterZone("alter zone a.test_zone set replicas = 2");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET (REPLICAS 2)";
        expectUnparsed(alterZoneSet, expectedStmt);
        expectUnparsed(alterZoneSetOneParam, expectedStmt);
        expectUnparsed(alterZoneSetOneEqParam, expectedStmt);
    }

    @Test
    public void alterZoneSetNodesFilter() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZone("alter zone a.test_zone set (NODES FILTER '\"SSD\"')");
        IgniteSqlAlterZoneSet alterZoneSetOneParam = parseAlterZone("alter zone a.test_zone set NODES FILTER '\"SSD\"'");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET (NODES FILTER '\"SSD\"')";
        expectUnparsed(alterZoneSet, expectedStmt);
        expectUnparsed(alterZoneSetOneParam, expectedStmt);
    }

    @Test
    public void alterZoneSetAutoScale() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZone("alter zone a.test_zone set (AUTO SCALE UP 2)");
        IgniteSqlAlterZoneSet alterZoneSetOneParam = parseAlterZone("alter zone a.test_zone set AUTO SCALE UP 2");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET (AUTO SCALE UP 2)";
        expectUnparsed(alterZoneSet, expectedStmt);
        expectUnparsed(alterZoneSetOneParam, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE IF EXISTS SET statement.
     */
    @Test
    public void alterZoneIfExistsSet() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZone("alter zone if exists a.test_zone set (replicas 2)");
        assertTrue(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" SET (REPLICAS 2)";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSetOptions() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZone(
                "alter zone a.test_zone set ("
                        + "REPLICAS 2, "
                        + "QUORUM SIZE 2, "
                        + "NODES FILTER '(\"US\" || \"EU\") && \"SSD\"', "
                        + "AUTO SCALE UP 2, "
                        + "AUTO SCALE DOWN 3)"
        );

        assertEquals(List.of("A", "TEST_ZONE"), alterZoneSet.name().names);
        assertNotNull(alterZoneSet.alterOptionsList());
        assertFalse(alterZoneSet.ifExists());

        List<SqlNode> optList = alterZoneSet.alterOptionsList().getList();

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.QUORUM_SIZE, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN, 3);

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET "
                + "(REPLICAS 2, "
                + "QUORUM SIZE 2, "
                + "NODES FILTER '(\"US\" || \"EU\") && \"SSD\"', "
                + "AUTO SCALE UP 2, "
                + "AUTO SCALE DOWN 3)";
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
    private static IgniteSqlCreateZone parseCreateZone(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlCreateZone.class, node);
    }

    /**
     * Parse ALTER ZONE SET statement.
     */
    private static IgniteSqlAlterZoneSet parseAlterZone(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneSet.class, node);
    }

    /**
     * Parse ALTER ZONE RENAME TO statement.
     */
    private static IgniteSqlAlterZoneRenameTo parseAlterZoneRenameTo(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneRenameTo.class, node);
    }

    static void assertThatZoneOptionPresent(List<SqlNode> optionList, ZoneOptionEnum name, Object expVal) {
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
