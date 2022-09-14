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

import java.util.List;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;

/**
 * Hash index descriptor builder.
 */
public interface PrimaryKeyDefinitionBuilder extends SchemaObjectBuilder {
    /**
     * Sets colocation columns.
     *
     * @param cols Colocation columns. Must be a valid subset of key columns.
     * @return Primary index builder.
     */
    PrimaryKeyDefinitionBuilder withColocationColumns(String... cols);

    /**
     * Sets colocation columns.
     *
     * @param cols Colocation columns. Must be a valid subset of key columns.
     * @return Primary index builder.
     */
    PrimaryKeyDefinitionBuilder withColocationColumns(List<String> cols);

    /**
     * Sets primary key columns.
     *
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    PrimaryKeyDefinitionBuilder withColumns(String... columns);

    /**
     * Sets primary key columns.
     *
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    PrimaryKeyDefinitionBuilder withColumns(List<String> columns);

    /**
     * Builds primary key.
     *
     * @return Primary key.
     */
    @Override
    PrimaryKeyDefinition build();
}
