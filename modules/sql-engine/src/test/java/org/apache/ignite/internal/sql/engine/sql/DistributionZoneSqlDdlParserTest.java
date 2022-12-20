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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
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

        assertThatZoneOptionPresent(optList, IgniteSqlCreateZoneOptionEnum.REPLICAS, 2);
        assertThatZoneOptionPresent(optList, IgniteSqlCreateZoneOptionEnum.PARTITIONS, 3);
        assertThatZoneOptionPresent(optList, IgniteSqlCreateZoneOptionEnum.AFFINITY_FUNCTION, "test_Affinity");
        assertThatZoneOptionPresent(optList, IgniteSqlCreateZoneOptionEnum.DATA_NODES_FILTER, "(\"US\" || \"EU\") && \"SSD\"");
        assertThatZoneOptionPresent(optList, IgniteSqlCreateZoneOptionEnum.DATA_NODES_AUTO_ADJUST, 1);

        SqlPrettyWriter w = new SqlPrettyWriter();
        createZone.unparse(w, 0, 0);

        assertThat(w.toString(), equalTo("CREATE ZONE \"TEST_ZONE\" WITH "
                + "REPLICAS = 2, "
                + "PARTITIONS = 3, "
                + "DATA_NODES_FILTER = '(\"US\" || \"EU\") && \"SSD\"', "
                + "AFFINITY_FUNCTION = 'test_Affinity', "
                + "DATA_NODES_AUTO_ADJUST = 1, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_UP = 2, "
                + "DATA_NODES_AUTO_ADJUST_SCALE_DOWN = 3"));
    }

    /**
     * Parse CREATE ZONE WITH unknown option.
     */
    @Test
    public void createZoneWithInvalidOptions() {
        // Unknown option.
        assertThrows(SqlParseException.class, () -> parseCreateZone("create zone test_zone with foo='bar'"));

        // Invalid option type.
        String query = "create zone test_zone with %s=%s";

        List<String> numericOptNames = Arrays.asList("PARTITIONS", "REPLICAS", "DATA_NODES_AUTO_ADJUST",
                "DATA_NODES_AUTO_ADJUST_SCALE_UP", "DATA_NODES_AUTO_ADJUST_SCALE_DOWN");

        for (String optName : numericOptNames) {
            assertThrows(SqlParseException.class, () -> parseCreateZone(String.format(query, optName, "'bar'")));
        }

        List<String> stringOptNames = Arrays.asList("AFFINITY_FUNCTION", "DATA_NODES_FILTER");

        for (String optName : stringOptNames) {
            assertThrows(SqlParseException.class, () -> parseCreateZone(String.format(query, optName, "1")));
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

        SqlPrettyWriter w = new SqlPrettyWriter();
        dropZone.unparse(w, 0, 0);

        assertThat(w.toString(), equalTo("DROP ZONE IF EXISTS \"TEST_ZONE\""));
    }

    /**
     * Parse CREATE ZONE statement.
     *
     * @param stmt Create zone query.
     * @return {@link org.apache.calcite.sql.SqlCreate SqlCreate} node.
     */
    private IgniteSqlCreateZone parseCreateZone(String stmt) throws SqlParseException {
        SqlNode node = parse(stmt);

        assertThat(node, instanceOf(IgniteSqlCreateZone.class));

        return (IgniteSqlCreateZone) node;
    }

    private void assertThatZoneOptionPresent(List<SqlNode> optionList, IgniteSqlCreateZoneOptionEnum name, Object expVal) {
        assertThat(optionList, Matchers.hasItem(ofTypeMatching(
                name + "=" + expVal,
                IgniteSqlCreateZoneOption.class,
                opt -> {
                    if (name == opt.key().symbolValue(IgniteSqlCreateZoneOptionEnum.class)) {
                        if (opt.value() instanceof SqlLiteral) {
                            return Objects.equals(expVal, ((SqlLiteral) opt.value()).getValueAs(expVal.getClass()));
                        }
                    }

                    return false;
                }
        )));
    }
}
