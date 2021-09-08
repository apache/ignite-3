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

package org.apache.ignite.schema;

import org.apache.ignite.internal.schema.definition.index.HashIndexBuilderImpl;
import org.apache.ignite.internal.schema.definition.index.PartialIndexBuilderImpl;
import org.apache.ignite.internal.schema.definition.index.PrimaryKeyBuilderImpl;
import org.apache.ignite.internal.schema.definition.index.SortedIndexBuilderImpl;
import org.apache.ignite.internal.schema.definition.table.TableColumnBuilderImpl;
import org.apache.ignite.internal.schema.definition.table.TableSchemaBuilderImpl;
import org.apache.ignite.schema.definition.index.HashIndexBuilder;
import org.apache.ignite.schema.definition.index.PartialIndexBuilder;
import org.apache.ignite.schema.definition.index.PrimaryKeyBuilder;
import org.apache.ignite.schema.definition.index.SortedIndexBuilder;
import org.apache.ignite.schema.definition.table.ColumnBuilder;
import org.apache.ignite.schema.definition.table.ColumnType;
import org.apache.ignite.schema.definition.table.TableSchema;
import org.apache.ignite.schema.definition.table.TableSchemaBuilder;

/**
 * Schema builder helper.
 */
public final class SchemaBuilders {
    /**
     * Create table descriptor from classes.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param keyClass Key class.
     * @param valueClass Value class.
     * @return Table descriptor for given key-value pair.
     */
    public static TableSchema buildSchema(String schemaName, String tableName, Class<?> keyClass, Class<?> valueClass) {
        // TODO IGNITE-13749: implement schema generation from classes.

        return null;
    }

    /**
     * Creates table descriptor builder.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @return Table descriptor builder.
     */
    public static TableSchemaBuilder tableBuilder(String schemaName, String tableName) {
        return new TableSchemaBuilderImpl(schemaName, tableName);
    }

    /**
     * Creates table column builder.
     *
     * @param name Column name.
     * @param type Column type.
     * @return Column builder.
     */
    public static ColumnBuilder column(String name, ColumnType type) {
        return new TableColumnBuilderImpl(name, type);
    }

    /**
     * Creates primary index builder.
     *
     * @return Primary index builder.
     */
    public static PrimaryKeyBuilder pkIndex() {
        return new PrimaryKeyBuilderImpl();
    }

    /**
     * Creates sorted index builder.
     *
     * @param name Index name.
     * @return Sorted index builder.
     */
    public static SortedIndexBuilder sortedIndex(String name) {
        return new SortedIndexBuilderImpl(name);
    }

    /**
     * Creates partial index builder.
     *
     * @param name Index name.
     * @return Partial index builder.
     */
    public static PartialIndexBuilder partialIndex(String name) {
        return new PartialIndexBuilderImpl(name);
    }

    /**
     * Creates hash index builder.
     *
     * @param name Index name.
     * @return Hash index builder.
     */
    public static HashIndexBuilder hashIndex(String name) {
        return new HashIndexBuilderImpl(name);
    }

    // Stub.
    private SchemaBuilders() {
    }
}
