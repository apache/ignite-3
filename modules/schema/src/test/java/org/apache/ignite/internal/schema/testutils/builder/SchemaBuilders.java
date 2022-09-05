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

package org.apache.ignite.internal.schema.testutils.builder;

import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;

/**
 * Schema builder helper.
 */
public final class SchemaBuilders {
    /**
     * Create table descriptor from classes.
     *
     * @param schemaName Schema name.
     * @param tableName  Table name.
     * @param keyClass   Key class.
     * @param valueClass Value class.
     * @return Table descriptor for given key-value pair.
     */
    public static TableDefinition buildSchema(String schemaName, String tableName, Class<?> keyClass, Class<?> valueClass) {
        // TODO IGNITE-13749: implement schema generation from classes.

        return null;
    }

    /**
     * Creates table descriptor builder.
     *
     * @param schemaName Schema name.
     * @param tableName  Table name.
     * @return Table descriptor builder.
     */
    public static TableDefinitionBuilder tableBuilder(String schemaName, String tableName) {
        return new TableDefinitionBuilderImpl(schemaName, tableName);
    }

    /**
     * Creates table column builder.
     *
     * @param name Column name.
     * @param type Column type.
     * @return Column builder.
     */
    public static ColumnDefinitionBuilder column(String name, ColumnType type) {
        return new ColumnDefinitionBuilderImpl(name, type);
    }

    /**
     * Creates primary key builder.
     *
     * @return Primary key builder.
     */
    public static PrimaryKeyDefinitionBuilder primaryKey() {
        return new PrimaryKeyDefinitionBuilderImpl();
    }

    /**
     * Creates sorted index builder.
     *
     * @param name Index name.
     * @return Sorted index builder.
     */
    public static SortedIndexDefinitionBuilder sortedIndex(String name) {
        return new SortedIndexDefinitionBuilderImpl(name);
    }

    /**
     * Creates hash index builder.
     *
     * @param name Index name.
     * @return Hash index builder.
     */
    public static HashIndexDefinitionBuilder hashIndex(String name) {
        return new HashIndexDefinitionBuilderImpl(name);
    }

    // Stub.
    private SchemaBuilders() {
    }
}
