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

import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.checkDuplicates;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.DdlSqlToCommandConverter.collectDataStorageNames;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
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
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.Type;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * For {@link DdlSqlToCommandConverter} testing.
 */
public class DdlSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {
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
    void testCheckDuplicates() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> checkDuplicates(
                        Set.of("replicas", "affinity"),
                        Set.of("partitions", "replicas")
                )
        );

        assertThat(exception.getMessage(), startsWith("Duplicate id: replicas"));

        assertDoesNotThrow(() -> checkDuplicates(
                        Set.of("replicas", "affinity"),
                        Set.of("replicas0", "affinity0")
                )
        );
    }

    @Test
    public void tableWithoutPkShouldThrowErrorWhenSysPropDefault() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "false")
    public void tableWithoutPkShouldThrowErrorWhenSysPropDisabled() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("Table without PRIMARY KEY is not supported"));
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void tableWithoutPkShouldInjectImplicitPkWhenSysPropEnabled() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) WITH STORAGE_PROFILE='" + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

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
        var node = parse("CREATE TABLE t (id varchar default gen_random_uuid primary key, val int) WITH STORAGE_PROFILE='"
                + DEFAULT_STORAGE_PROFILE + "'");

        assertThat(node, instanceOf(SqlDdl.class));

        var cmd = converter.convert((SqlDdl) node, createContext());

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
    public void tableWithoutStorageProfileShouldThrowError() throws SqlParseException {
        var node = parse("CREATE TABLE t (val int) with storage_profile=''");

        assertThat(node, instanceOf(SqlDdl.class));

        var ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) node, createContext())
        );

        assertThat(ex.getMessage(), containsString("String cannot be empty"));

        var newNode = parse("CREATE TABLE t (val int) WITH PRIMARY_ZONE='ZONE', storage_profile=''");

        assertThat(node, instanceOf(SqlDdl.class));

        ex = assertThrows(
                IgniteException.class,
                () -> converter.convert((SqlDdl) newNode, createContext())
        );

        assertThat(ex.getMessage(), containsString("String cannot be empty"));
    }

    private static Matcher<ColumnDefinition> columnThat(String description, Function<ColumnDefinition, Boolean> checker) {
        return new CustomMatcher<>(description) {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof ColumnDefinition && checker.apply((ColumnDefinition) actual) == Boolean.TRUE;
            }
        };
    }
}
