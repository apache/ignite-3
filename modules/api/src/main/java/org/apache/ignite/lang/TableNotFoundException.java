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

import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Exception is thrown when a specified table cannot be found.
 */
public class TableNotFoundException extends IgniteException {
    /**
     * Creates an exception with the given table name.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public TableNotFoundException(String schemaName, String tableName) {
        super(TABLE_NOT_FOUND_ERR, "The table does not exist [name=" + canonicalName(schemaName, tableName) + ']');
    }

    /**
     * Creates an exception with the given canonical table name.
     *
     * @param tableName Table name.
     */
    public TableNotFoundException(String tableName) {
        super(TABLE_NOT_FOUND_ERR, "The table does not exist [name=" + tableName + ']');
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of the exception.
     * @param code Full error code.
     * @param message Detailed message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TableNotFoundException(UUID traceId, int code, String message, @Nullable Throwable cause) {
        super(traceId, code, message, cause);
    }
}
