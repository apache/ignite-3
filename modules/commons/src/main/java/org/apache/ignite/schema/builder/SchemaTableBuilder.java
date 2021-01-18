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

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;

/**
 * Table descriptor builder.
 */
public interface SchemaTableBuilder {
    /** Primary key index name. */
    public static final String PRIMARY_KEY_INDEX_NAME = "PK";

    /**
     * @param columns Table columns.
     * @return Schema table builder for chaining.
     */
    SchemaTableBuilder columns(Column... columns);

    /**
     * @param colNames PK column names.
     * @return Schema table builder for chaining.
     */
    SchemaTableBuilder pkColumns(String... colNames);

    /**
     * @param colNames Affinity column names.
     * @return Schema table builder for chaining.
     */
    SchemaTableBuilder affinityColumns(String... colNames);

    /**
     * Adds index.
     *
     * @param index Table index.
     * @return Schema table builder for chaining.
     */
    SchemaTableBuilder withindex(TableIndex index);

    /**
     * Builds table schema.
     *
     * @return Table schema.
     */
    SchemaTable build();
}
