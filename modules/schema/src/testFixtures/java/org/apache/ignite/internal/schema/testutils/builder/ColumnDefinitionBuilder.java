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

import java.util.Map;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.jetbrains.annotations.Nullable;

/**
 * Table column builder.
 */
public interface ColumnDefinitionBuilder extends SchemaObjectBuilder {
    /**
     * Set nullable attribute.
     *
     * @param nullable If {@code false}, null values will not be allowed for this column.
     *
     * @return {@code this} for chaining.
     */
    ColumnDefinitionBuilder asNullable(boolean nullable);

    /**
     * Sets column default value expression.
     *
     * @param defValExpr Default value expression.
     * @return {@code this} for chaining.
     */
    ColumnDefinitionBuilder withDefaultValue(@Nullable Object defValExpr);

    /** {@inheritDoc} */
    @Override
    ColumnDefinitionBuilder withHints(Map<String, String> hints);

    /**
     * Builds column.
     *
     * @return Built column.
     */
    @Override
    ColumnDefinition build();
}

