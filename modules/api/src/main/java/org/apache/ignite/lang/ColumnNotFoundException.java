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

package org.apache.ignite.lang;

import static org.apache.ignite.internal.util.IgniteNameUtils.canonicalName;
import static org.apache.ignite.internal.util.IgniteNameUtils.quote;
import static org.apache.ignite.lang.ErrorGroups.Table.COLUMN_NOT_FOUND_ERR;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.UUID;

/**
 * Exception is thrown when appropriate column is not found.
 */
public class ColumnNotFoundException extends IgniteException {
    /**
     * Create a new exception with given column name.
     *
     * @param columnName Column name.
     */
    public ColumnNotFoundException(String columnName) {
        super(COLUMN_NOT_FOUND_ERR, format("Column does not exist [name={}]", quote(columnName)));
    }

    /**
     * Create a new exception with given column name.
     *
     * @param schemaName Name of the schema the table belongs to.
     * @param tableName Name of the table the column belongs to.
     * @param columnName Name of the column of interest.
     */
    public ColumnNotFoundException(String schemaName, String tableName, String columnName) {
        super(COLUMN_NOT_FOUND_ERR, format("Column does not exist [tableName={}, columnName={}]",
                canonicalName(schemaName, tableName), quote(columnName)));
    }

    /**
     * Creates a new exception with the given trace id, error code, detail message and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ColumnNotFoundException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
