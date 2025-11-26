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

package org.apache.ignite.catalog.definitions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link TableDefinition}.
 */
public class TableDefinitionTest {

    @Test
    public void tableNameMustBeNotNull() {
        var err = assertThrows(NullPointerException.class, () -> TableDefinition.builder((String) null));
        assertEquals("Table name must not be null.", err.getMessage());
    }

    @Test
    public void qualifiedNameMustBeNotNull() {
        var err = assertThrows(NullPointerException.class, () -> TableDefinition.builder((QualifiedName) null));
        assertEquals("Qualified table name must not be null.", err.getMessage());
    }

    @ParameterizedTest
    @MethodSource("schemaAndTable")
    public void testFromTableNameThenSetSchema(String schema, String table, QualifiedName expected) {
        TableDefinition def = TableDefinition.builder(table)
                .schema(schema)
                .build();

        QualifiedName qualifiedName = def.qualifiedName();
        assertEquals(expected, qualifiedName);
        assertEquals(IgniteNameUtils.quoteIfNeeded(qualifiedName.schemaName()), def.schemaName());
        assertEquals(IgniteNameUtils.quoteIfNeeded(qualifiedName.objectName()), def.tableName());

        TableDefinition def2 = def.toBuilder().schema(schema).build();
        assertEquals(def.qualifiedName(), def2.qualifiedName());
        assertEquals(def.schemaName(), def2.schemaName());
        assertEquals(def.tableName(), def2.tableName());

        TableDefinition def3 = TableDefinition.builder(def.qualifiedName()).build();
        assertEquals(def.qualifiedName(), def3.qualifiedName());
        assertEquals(def.schemaName(), def3.schemaName());
        assertEquals(def.tableName(), def3.tableName());
    }

    @ParameterizedTest
    @MethodSource("schemaAndTable")
    public void testFromQualifiedNameThenSchema(String schema, String table, QualifiedName expected) {
        QualifiedName initialName = QualifiedName.of("ABC", table);
        TableDefinition def = TableDefinition.builder(initialName)
                .schema(schema)
                .build();

        QualifiedName qualifiedName = def.qualifiedName();
        assertEquals(expected, qualifiedName);
        assertEquals(IgniteNameUtils.quoteIfNeeded(qualifiedName.schemaName()), def.schemaName());
        assertEquals(IgniteNameUtils.quoteIfNeeded(qualifiedName.objectName()), def.tableName());

        TableDefinition def2 = def.toBuilder().schema(schema).build();
        assertEquals(def.qualifiedName(), def2.qualifiedName());
        assertEquals(def.schemaName(), def2.schemaName());
        assertEquals(def.tableName(), def2.tableName());

        TableDefinition def3 = TableDefinition.builder(def.qualifiedName()).build();
        assertEquals(def.qualifiedName(), def3.qualifiedName());
        assertEquals(def.schemaName(), def3.schemaName());
        assertEquals(def.tableName(), def3.tableName());
    }

    private static Stream<Arguments> schemaAndTable() {
        return Stream.of(
                Arguments.of("s", "a", QualifiedName.of("S", "A")),
                Arguments.of("\"a Schema\"", "a", QualifiedName.of("\"a Schema\"", "A")),
                Arguments.of("\"a Schema\"", "\"a table\"", QualifiedName.of("\"a Schema\"", "\"a table\"")),
                Arguments.of("\"aSchema\"", "\"aTable\"", QualifiedName.of("\"aSchema\"", "\"aTable\"")),
                Arguments.of("\"a Schema\"", "\"atable\"", QualifiedName.of("\"a Schema\"", "\"atable\"")),

                Arguments.of("\"aSchema\"", "\"aTable \"\"X\"\"  \"", QualifiedName.of("\"aSchema\"", "\"aTable \"\"X\"\"  \"")),
                Arguments.of("\"aSchema\"", "\"\"\"atable\"\"\"", QualifiedName.of("\"aSchema\"", "\"\"\"atable\"\"\"")),
                Arguments.of("\"aSchema\"", "\"\"\"aTable\"\"\"", QualifiedName.of("\"aSchema\"", "\"\"\"aTable\"\"\"")),
                Arguments.of("\"aSchema\"", "\"\"\"ATABLE\"\"\"", QualifiedName.of("\"aSchema\"", "\"\"\"ATABLE\"\"\""))
        );
    }

    @ParameterizedTest
    @MethodSource("overwriteSchemaArgs")
    public void testFromTableNamePreservesTableNameWithSchemaChanges(String tableName, QualifiedName expected) {
        {
            TableDefinition def = TableDefinition.builder(tableName)
                    .schema("A")
                    .schema("B")
                    .build();
            assertEquals(expected, def.qualifiedName(), "overwrite once");
        }
        {
            TableDefinition def = TableDefinition.builder(tableName)
                    .schema("X")
                    .schema("A")
                    .schema("B")
                    .build();
            assertEquals(expected, def.qualifiedName(), "overwrite twice");
        }
    }

    @ParameterizedTest
    @MethodSource("overwriteSchemaArgs")
    public void fromQualifiedNamePreservesTableNameWithSchemaChanges(String tableName, QualifiedName expected) {
        QualifiedName initialName = QualifiedName.of("S", tableName);
        {
            TableDefinition def = TableDefinition.builder(initialName)
                    .schema("A")
                    .schema("B")
                    .build();
            assertEquals(expected, def.qualifiedName(), "overwrite once");
        }
        {
            TableDefinition def = TableDefinition.builder(initialName)
                    .schema("X")
                    .schema("A")
                    .schema("B")
                    .build();
            assertEquals(expected, def.qualifiedName(), "overwrite twice");
        }
    }

    private static Stream<Arguments> overwriteSchemaArgs() {
        return Stream.of(
                Arguments.of("A", QualifiedName.of("B", "A")),
                Arguments.of("Abc", QualifiedName.of("B", "ABC")),
                Arguments.of("\"Abc\"", QualifiedName.of("B", "\"Abc\"")),
                Arguments.of("\"abc\"", QualifiedName.of("B", "\"abc\"")),
                Arguments.of("\"Abc \"\"0\"\"  \"", QualifiedName.of("B", "\"Abc \"\"0\"\"  \"")),
                Arguments.of("\"abc \"\"0\"\"  \"", QualifiedName.of("B", "\"abc \"\"0\"\"  \""))
        );
    }
}
