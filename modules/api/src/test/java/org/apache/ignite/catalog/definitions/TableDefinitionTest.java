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

import java.util.stream.Stream;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link TableDefinition}.
 */
public class TableDefinitionTest {

    @ParameterizedTest
    @MethodSource("schemaAndTable")
    public void testFromTableNameThenSetSchema(
            String schema,
            String table,
            QualifiedName expected,
            String normSchema,
            String normTable
    ) {
        TableDefinition def = TableDefinition.builder(table)
                .schema(schema)
                .build();

        assertEquals(expected, def.qualifiedName());
        assertEquals(normSchema, def.schemaName());
        assertEquals(normTable, def.tableName());
    }

    @ParameterizedTest
    @MethodSource("schemaAndTable")
    public void testFromQualifiedNameThenSchema(
            String schema,
            String table,
            QualifiedName expected,
            String normSchema,
            String normTable
    ) {
        QualifiedName qualifiedName = QualifiedName.of("ABC", table);
        TableDefinition def = TableDefinition.builder(qualifiedName)
                .schema(schema)
                .build();

        assertEquals(expected, def.qualifiedName());
        assertEquals(normSchema, def.schemaName());
        assertEquals(normTable, def.tableName());
    }

    @ParameterizedTest
    @MethodSource("schemaAndTable")
    public void testFromQualifiedNameThenSchemaThenQualifiedName(
            String schema,
            String table,
            QualifiedName expected,
            String normSchema,
            String normTable
    ) {
        QualifiedName qualifiedName = QualifiedName.of("ABC", "DEF");
        TableDefinition def = TableDefinition.builder(qualifiedName)
                .schema(schema)
                .qualifiedName(QualifiedName.of(schema, table))
                .build();

        assertEquals(expected, def.qualifiedName());
        assertEquals(normSchema, def.schemaName());
        assertEquals(normTable, def.tableName());
    }

    private static Stream<Arguments> schemaAndTable() {
        return Stream.of(
                Arguments.of("s", "a", QualifiedName.of("S", "A"), "S", "A"),
                Arguments.of("\"a Schema\"", "a",  QualifiedName.of("\"a Schema\"", "A"), "a Schema", "A"),
                Arguments.of("\"a Schema\"", "\"a table\"", QualifiedName.of("\"a Schema\"", "\"a table\""), "a Schema", "a table"),
                Arguments.of("\"aSchema\"", "\"aTable\"", QualifiedName.of("\"aSchema\"", "\"aTable\""), "aSchema", "aTable"),

                Arguments.of("\"aSchema\"", "\"aTable \"\"X\"\"  \"", QualifiedName.of("\"aSchema\"", "\"aTable \"\"X\"\"  \""),
                        "aSchema", "aTable \"X\"  ")
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
                Arguments.of("\"Abc \"\"0\"\"  \"", QualifiedName.of("B", "\"Abc \"\"0\"\"  \""))
        );
    }
}
