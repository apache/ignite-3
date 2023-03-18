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

package org.apache.ignite.schema.definition;

/**
 * Table column descriptor.
 */
public interface ColumnDefinition {
    /**
     * Returns a column name.
     *
     * @return Column name.
     */
    String name();

    /**
     * Returns a column type.
     *
     * @return Column type.
     */
    ColumnType type();

    /**
     * Returns a {@code Nullable} flag value.
     *
     * @return {@code True} if null-values are allowed, {@code false} otherwise.
     */
    boolean nullable();

    /**
     * Returns a default value definition.
     *
     * @param <T> Subtype of the definition.
     * @return Default value definition.
     */
    <T extends DefaultValueDefinition> T defaultValueDefinition();
}
