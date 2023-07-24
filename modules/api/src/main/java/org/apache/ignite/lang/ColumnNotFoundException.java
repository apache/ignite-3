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

import static org.apache.ignite.lang.ErrorGroups.Table.COLUMN_NOT_FOUND_ERR;
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;
import static org.apache.ignite.lang.util.IgniteNameUtils.quote;

import java.util.UUID;

/**
 * Exception is thrown when the indicated column is not found.
 */
public class ColumnNotFoundException extends IgniteException {
    /**
     * Creates an exception with a given column name.
     *
     * @param columnName Column name.
     */
    public ColumnNotFoundException(String columnName) {
        super(COLUMN_NOT_FOUND_ERR, "Column does not exist [name=" + quote(columnName) + ']');
    }

    /**
     * Creates an exception with a given column name.
     *
     * @param schemaName Name of the schema the table belongs to.
     * @param columnName Column name.
     * @param tableName Table name.
     */
    public ColumnNotFoundException(String schemaName, String tableName, String columnName) {
        super(
                COLUMN_NOT_FOUND_ERR,
                "Column does not exist [tableName=" + canonicalName(schemaName, tableName) + ", columnName=" + quote(columnName) + ']');
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ColumnNotFoundException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
