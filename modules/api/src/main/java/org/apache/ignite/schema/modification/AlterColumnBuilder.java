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

package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.definition.ColumnType;

/**
 * Alter column builder.
 *
 * <p>NOTE: Only safe actions, which can be applied automatically on-the-fly, are allowed.
 */

//The above description is titally unclear.
public interface AlterColumnBuilder {
    /**
     * Renames a column.
     *
     * @param newName New column name.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewName(String newName);

    /**
     * Converts a column to a new type.
     *
     * <p>Note: The new column type must be compatible with the old one.
     *
     * @param newType New column type.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder convertTo(ColumnType newType);

    /**
     * Sets a new column default value.
     *
     * @param defaultValue Default value.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewDefault(Object defaultValue);

    /**
     * Sets the nullability attribute.
     *
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNullable();

    /**
     * Marks a column as non-nullable.
     *
     * <p>Note: A replacement parameter is mandatory; all previously stored 'nulls' will be treated as a replacement value by the read operation.
     *
     * @param replacement Non-null value to convert the 'null' to.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNonNullable(Object replacement);

    /**
     * Builds alter column descriptor and passes it to the parent table modification builder.
     *
     * @return Parent builder.
     */
    TableModificationBuilder done();
}
//What is "alter" column descriptor?