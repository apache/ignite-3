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

package org.apache.ignite.internal.catalog.commands;

import org.apache.ignite.sql.ColumnType;

/**
 * Builder of a command that changes a column in a particular table.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface AlterTableAlterColumnCommandBuilder extends AbstractTableCommandBuilder<AlterTableAlterColumnCommandBuilder> {
    /** A name of the column of interest. Should not be null or blank. */
    AlterTableAlterColumnCommandBuilder columnName(String columnName);

    /** A new type of the column. Optional. */
    AlterTableAlterColumnCommandBuilder type(ColumnType type);

    /** A new precision of the column. Optional. */
    AlterTableAlterColumnCommandBuilder precision(int precision);

    /** A new length of the column. Optional. */
    AlterTableAlterColumnCommandBuilder length(int length);

    /** A new scale of the column. Optional. */
    AlterTableAlterColumnCommandBuilder scale(int scale);

    /** A new precision of the column. Optional. */
    AlterTableAlterColumnCommandBuilder nullable(boolean nullable);

    /** A new precision of the column. Optional. */
    AlterTableAlterColumnCommandBuilder deferredDefaultValue(DeferredDefaultValue deferredDefault);
}
