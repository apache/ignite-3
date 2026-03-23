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

import static org.apache.ignite.internal.sql.engine.sql.DistributionZoneSqlDdlParserTest.assertThatZoneOptionPresent;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify parsing of the DDL "ZONE" commands contains WITH syntax.
 */
public class DistributionZoneObsoleteSyntaxSqlDdlParserTest extends AbstractParserTest {
    /**
     * Parse simple CREATE ZONE statement.
     */
    @Test
    public void createZoneNoOptions() {
        // Simple name.
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone with storage_profiles = 'p'");

        assertThat(createZone.name().names, is(List.of("TEST_ZONE")));
        assertFalse(createZone.ifNotExists());
        assertNotNull(createZone.createOptionList());
        assertNotNull(createZone.storageProfiles());
        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" STORAGE PROFILES['p']");

        // Fully qualified name.
        createZone = parseCreateZone("create zone public.test_zone with storage_profiles = 'p'");
        assertThat(createZone.name().names, is(List.of("PUBLIC", "TEST_ZONE")));
        expectUnparsed(createZone, "CREATE ZONE \"PUBLIC\".\"TEST_ZONE\" STORAGE PROFILES['p']");

        // Quoted identifier.
        createZone = parseCreateZone("create zone \"public\".\"test_Zone\" with storage_profiles = 'p'");
        assertThat(createZone.name().names, is(List.of("public", "test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public\".\"test_Zone\" STORAGE PROFILES['p']");

        createZone = parseCreateZone("create zone \"public-test_Zone\" with storage_profiles = 'p'");
        assertThat(createZone.name().names, is(List.of("public-test_Zone")));
        expectUnparsed(createZone, "CREATE ZONE \"public-test_Zone\" STORAGE PROFILES['p']");
    }

    /**
     * Parse CREATE ZONE IF NOT EXISTS WITH statement.
     */
    @Test
    public void createZoneIfNotExists() {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone if not exists test_zone with storage_profiles = 'p'");

        assertTrue(createZone.ifNotExists());
        assertNotNull(createZone.createOptionList());
        assertNotNull(createZone.storageProfiles());

        expectUnparsed(createZone, "CREATE ZONE IF NOT EXISTS \"TEST_ZONE\" STORAGE PROFILES['p']");
    }

    /**
     * Parse CREATE ZONE WITH ... statement.
     */
    @Test
    public void createZoneWithOptions() {
        IgniteSqlCreateZone createZone = parseCreateZone(
                "create zone test_zone with "
                        + "storage_profiles='default, new', "
                        + "replicas=2, "
                        + "partitions=3, "
                        + "data_nodes_filter='(\"US\" || \"EU\") && \"SSD\"', "
                        + "distribution_algorithm='test_Distribution', "
                        + "data_nodes_auto_adjust_scale_up=2, "
                        + "data_nodes_auto_adjust_scale_down=3,"
                        + "consistency_mode='HIGH_AVAILABILITY'"
        );

        assertNotNull(createZone.createOptionList());

        assertThat(createZone.storageProfiles(), not(nullValue()));
        assertThat(
                createZone.storageProfiles(),
                hasItems(
                        ofTypeMatching("profile with name 'default'", SqlNode.class,
                                literal -> "default".equals(((SqlLiteral) literal).getValueAs(String.class))),
                        ofTypeMatching("profile with name 'new'", SqlNode.class, 
                                literal -> "new".equals(((SqlLiteral) literal).getValueAs(String.class)))
                )
        );

        List<SqlNode> optList = createZone.createOptionList().getList();

        assertThat(optList.size(), is(7));
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.PARTITIONS, 3);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DISTRIBUTION_ALGORITHM, "test_Distribution");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN, 3);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.CONSISTENCY_MODE, "HIGH_AVAILABILITY");

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" ("
                + "REPLICAS 2, "
                + "PARTITIONS 3, "
                + "NODES FILTER '(\"US\" || \"EU\") && \"SSD\"', "
                + "DISTRIBUTION ALGORITHM 'test_Distribution', "
                + "AUTO SCALE UP 2, "
                + "AUTO SCALE DOWN 3, "
                + "CONSISTENCY MODE 'HIGH_AVAILABILITY') "
                + "STORAGE PROFILES['default', 'new']");
    }

    @Test
    public void createZoneWithInvalidOption() {
        IgniteSqlCreateZone createZone = parseCreateZone(
                "create zone test_zone with non_existing_option=1"
        );

        assertNotNull(createZone.createOptionList());

        expectUnparsed(
                createZone,
                "CREATE ZONE \"TEST_ZONE\" (\"NON_EXISTING_OPTION\" 1)");
    }

    /**
     * Parse CREATE ZONE WITH ... statement.
     */
    @Test
    public void createZoneWithAllReplicas() {
        IgniteSqlCreateZone createZone = parseCreateZone("create zone test_zone with replicas=ALL");

        assertNotNull(createZone.createOptionList());

        List<SqlNode> optList = createZone.createOptionList().getList();

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, IgniteSqlZoneOptionMode.ALL);

        expectUnparsed(createZone, "CREATE ZONE \"TEST_ZONE\" (REPLICAS ALL)");
    }

    /**
     * Parsing ALTER ZONE SET statement.
     */
    @Test
    public void alterZoneSet() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone a.test_zone set replicas=2");
        assertFalse(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET (REPLICAS 2)";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Parsing ALTER ZONE IF EXISTS SET statement.
     */
    @Test
    public void alterZoneIfExistsSet() {
        IgniteSqlAlterZoneSet alterZoneSet = parseAlterZoneSet("alter zone if exists a.test_zone set replicas=2");
        assertTrue(alterZoneSet.ifExists());

        String expectedStmt = "ALTER ZONE IF EXISTS \"A\".\"TEST_ZONE\" SET (REPLICAS 2)";
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
                        + "data_nodes_auto_adjust_scale_up=2, "
                        + "data_nodes_auto_adjust_scale_down=3"
        );

        assertEquals(List.of("A", "TEST_ZONE"), alterZoneSet.name().names);
        assertNotNull(alterZoneSet.alterOptionsList());
        assertFalse(alterZoneSet.ifExists());

        List<SqlNode> optList = alterZoneSet.alterOptionsList().getList();

        assertThatZoneOptionPresent(optList, ZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, ZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");

        String expectedStmt = "ALTER ZONE \"A\".\"TEST_ZONE\" SET ("
                + "REPLICAS 2, "
                + "NODES FILTER '(\"US\" || \"EU\") && \"SSD\"', "
                + "AUTO SCALE UP 2, "
                + "AUTO SCALE DOWN 3)";
        expectUnparsed(alterZoneSet, expectedStmt);
    }

    /**
     * Ensures that we cannot change zone parameters and set this zone as default in the same request.
     */
    @Test
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
    private static IgniteSqlAlterZoneSet parseAlterZoneSet(String stmt) {
        SqlNode node = parse(stmt);

        return assertInstanceOf(IgniteSqlAlterZoneSet.class, node);
    }
}
