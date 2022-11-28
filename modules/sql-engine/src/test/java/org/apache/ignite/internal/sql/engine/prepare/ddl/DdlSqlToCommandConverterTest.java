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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.checkDuplicates;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.collectDataStorageNames;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.collectDdlOptionInfos;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * For {@link DdlSqlToCommandConverter} testing.
 */
public class DdlSqlToCommandConverterTest extends BaseIgniteAbstractTest {
    @AfterAll
    public static void resetStaticState() {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);
    }

    @Test
    void testCollectDataStorageNames() {
        assertThat(collectDataStorageNames(Set.of()), equalTo(Map.of()));

        assertThat(
                collectDataStorageNames(Set.of("rocksdb")),
                equalTo(Map.of("ROCKSDB", "rocksdb"))
        );

        assertThat(
                collectDataStorageNames(Set.of("ROCKSDB")),
                equalTo(Map.of("ROCKSDB", "ROCKSDB"))
        );

        assertThat(
                collectDataStorageNames(Set.of("rocksDb", "pageMemory")),
                equalTo(Map.of("ROCKSDB", "rocksDb", "PAGEMEMORY", "pageMemory"))
        );

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> collectDataStorageNames(Set.of("rocksdb", "rocksDb"))
        );

        assertThat(exception.getMessage(), startsWith("Duplicate key"));
    }

    @Test
    void testCollectTableOptionInfos() {
        assertThat(collectDdlOptionInfos(new DdlOptionInfo[0]), equalTo(Map.of()));

        DdlOptionInfo<CreateTableCommand, ?> replicas = tableOptionInfo("replicas");

        assertThat(
                collectDdlOptionInfos(replicas),
                equalTo(Map.of("REPLICAS", replicas))
        );

        replicas = tableOptionInfo("REPLICAS");

        assertThat(
                collectDdlOptionInfos(replicas),
                equalTo(Map.of("REPLICAS", replicas))
        );

        replicas = tableOptionInfo("replicas");
        DdlOptionInfo<CreateTableCommand, ?> partitions = tableOptionInfo("partitions");

        assertThat(
                collectDdlOptionInfos(replicas, partitions),
                equalTo(Map.of("REPLICAS", replicas, "PARTITIONS", partitions))
        );

        DdlOptionInfo<CreateTableCommand, ?> replicas0 = tableOptionInfo("replicas");
        DdlOptionInfo<CreateTableCommand, ?> replicas1 = tableOptionInfo("REPLICAS");

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> collectDdlOptionInfos(replicas0, replicas1)
        );

        assertThat(exception.getMessage(), startsWith("Duplicate key"));
    }

    @Test
    void testCheckPositiveNumber() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> checkDuplicates(
                        collectDdlOptionInfos(tableOptionInfo("replicas")),
                        collectDdlOptionInfos(tableOptionInfo("replicas"))
                )
        );

        assertThat(exception.getMessage(), startsWith("Duplicate id"));

        assertDoesNotThrow(() -> checkDuplicates(
                collectDdlOptionInfos(tableOptionInfo("replicas")),
                collectDdlOptionInfos(tableOptionInfo("partitions"))
        ));
    }

    @Test
    public void tableWithoutPkShouldThrowErrorWhenSysPropDefault() throws SqlParseException {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);

        var node = parse("CREATE TABLE t (val int)");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "false")
    public void tableWithoutPkShouldThrowErrorWhenSysPropDisabled() throws SqlParseException {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);

        var node = parse("CREATE TABLE t (val int)");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithoutPkShouldInjectImplicitPkWhenSysPropEnabled() throws SqlParseException {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);

        var node = parse("CREATE TABLE t (val int)");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertThat(
                createTable.columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("implicit PK col", cd -> Commons.IMPLICIT_PK_COL_NAME.equals(cd.name())
                                && !cd.nullable() && SqlTypeName.VARCHAR.equals(cd.type().getSqlTypeName())))
                )
        );

        assertThat(
                createTable.primaryKeyColumns(),
                hasSize(1)
        );

        assertThat(
                createTable.primaryKeyColumns(),
                hasItem(Commons.IMPLICIT_PK_COL_NAME)
        );
    }

    @Test
    public void tableWithAutogenPkColumn() throws SqlParseException {
        var node = parse("CREATE TABLE t (id varchar default gen_random_uuid primary key, val int)");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateTableCommand.class));

        var createTable = (CreateTableCommand) cmd;

        assertThat(
                createTable.columns(),
                allOf(
                        hasItem(columnThat("column with name \"VAL\"", cd -> "VAL".equals(cd.name()))),
                        hasItem(columnThat("PK with functional default",
                                cd -> "ID".equals(cd.name())
                                        && !cd.nullable()
                                        && SqlTypeName.VARCHAR.equals(cd.type().getSqlTypeName())
                                        && cd.defaultValueDefinition().type() == Type.FUNCTION_CALL
                                        && "GEN_RANDOM_UUID".equals(((FunctionCall) cd.defaultValueDefinition()).functionName())
                                )
                        )
                )
        );
    }

    @Test
    public void zoneCreate() throws SqlParseException {
        var node = parse("CREATE ZONE test");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext());

        assertThat(cmd, Matchers.instanceOf(CreateZoneCommand.class));
    }

    @Test
    public void zoneCreateOptions() throws SqlParseException {
        SqlNode node = parse("CREATE ZONE test with "
                + "partitions=2, "
                + "replicas=3, "
                + "affinity_function='rendezvous', "
                + "data_nodes_filter='\"attr1\" && \"attr2\"', "
                + "data_nodes_auto_adjust=100");

        assertThat(node, instanceOf(SqlDdl.class));

        DdlCommand cmd = new DdlSqlToCommandConverter(Map.of(), () -> "default").convert((SqlDdl) node, createContext());

        CreateZoneCommand createZone = (CreateZoneCommand) cmd;

        assertThat(createZone.partitions(), equalTo(2));
        assertThat(createZone.replicas(), equalTo(3));
        assertThat(createZone.affinity(), equalTo("rendezvous"));
        assertThat(createZone.nodeFilter(), equalTo("\"attr1\" && \"attr2\""));
        assertThat(createZone.dataNodesAutoAdjustScaleUp(), equalTo(100));
        assertThat(createZone.dataNodesAutoAdjustScaleDown(), equalTo(100));
    }

    private DdlOptionInfo<CreateTableCommand, ?> tableOptionInfo(String name) {
        return new DdlOptionInfo<>(name, Object.class, null, (createTableCommand, o) -> {
        });
    }

    private static Matcher<ColumnDefinition> columnThat(String description, Function<ColumnDefinition, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof ColumnDefinition && checker.apply((ColumnDefinition) actual) == Boolean.TRUE;
            }
        };
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

    private static PlanningContext createContext() {
        var schemaName = "PUBLIC";
        var schema = Frameworks.createRootSchema(false).add(schemaName, new IgniteSchema(schemaName));

        return PlanningContext.builder()
                .parentContext(BaseQueryContext.builder()
                        .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                                .defaultSchema(schema)
                                .build())
                        .build())
                .query("")
                .build();
    }
}
