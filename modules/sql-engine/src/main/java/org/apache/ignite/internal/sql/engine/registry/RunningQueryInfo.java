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

package org.apache.ignite.internal.sql.engine.registry;

import java.time.Instant;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.jetbrains.annotations.Nullable;

/**
 * Information about the running query, which is exposed to the {@code SQL_QUERIES} system view.
 *
 * <p>Some fields are updated during query execution and the information in them may not be available
 * until a particular execution phase occurs.
 */
public interface RunningQueryInfo {
    /** Returns the query identifier. */
    UUID queryId();

    /** Returns the schema name. */
    String schema();

    /** Returns the original SQL query text. */
    String sql();

    /** Returns the query start time. */
    Instant startTime();

    /** Returns the query execution phase. */
    String phase();

    /** Sets the query execution phase. */
    void phase(QueryExecutionPhase phase);

    /** Returns the query type, or {@code null} if the query type is not yet known. */
    @Nullable String queryType();

    /** Sets the query type. */
    void queryType(SqlQueryType queryType);

    /** Returns the transaction ID or {@code null} if the transaction has not been started. */
    @Nullable String transactionId();

    /** Sets the transaction identifier. */
    void transactionId(UUID txId);

    /** Returns the cursor associated with the query or {@code null} if no cursor was opened. */
    @Nullable AsyncSqlCursor<?> cursor();

    /** Sets the open cursor associated with the query. */
    void cursor(AsyncSqlCursor<?> cursor);

    /** Returns the ID of the parent multi-statement query or {@code null} if the query is not a part of multi-statement query. */
    @Nullable String parentId();

    /** Returns the statement number in the multi-statement query or {@code null} if the query is not a part of multi-statement query. */
    @Nullable Integer statementNumber();
}
