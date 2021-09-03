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

package org.apache.ignite.schema.builder;

import java.util.Map;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.TableSchema;
import org.apache.ignite.schema.TableIndex;

/**
 * Table descriptor builder.
 */
public interface TableSchemaBuilder extends SchemaObjectBuilder {
    /**
     * Adds columns to the table.
     *
     * @param columns Table columns.
     * @return {@code This} for chaining.
     */
    TableSchemaBuilder columns(Column... columns);

    /**
     * Adds an index.
     *
     * @param index Table index.
     * @return {@code This} for chaining.
     */
    TableSchemaBuilder withIndex(TableIndex index);

    /**
     * Shortcut method for adding {@link PrimaryIndex} via {@link #withIndex(TableIndex)}.
     *
     * @param colName Key column name.
     * @return {@code This} for chaining.
     */
    TableSchemaBuilder withPrimaryKey(String colName);

    /** {@inheritDoc} */
    @Override TableSchemaBuilder withHints(Map<String, String> hints);

    /**
     * Builds table.
     *
     * @return Table.
     */
    @Override TableSchema build();

}
