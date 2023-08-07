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

import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Index;

/**
 * Exception is thrown when the specified index is not found.
 */
public class IndexNotFoundException extends IgniteException {
    /**
     * Creates an exception with the given index name.
     *
     * @param schemaName Schema name.
     * @param indexName Index name.
     */
    public IndexNotFoundException(String schemaName, String indexName) {
        super(Index.INDEX_NOT_FOUND_ERR, "Index does not exist [name=" + canonicalName(schemaName, indexName) + ']');
    }

    /**
     * Creates an exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IndexNotFoundException(UUID traceId, String message, Throwable cause) {
        super(traceId, Index.INDEX_NOT_FOUND_ERR, message, cause);
    }

    /**
     * Creates a new exception with the given trace ID, error code, detailed message, and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IndexNotFoundException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
