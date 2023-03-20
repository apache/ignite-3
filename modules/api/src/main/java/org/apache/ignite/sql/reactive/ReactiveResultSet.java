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

package org.apache.ignite.sql.reactive;

import java.util.concurrent.Flow;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;

/**
 * Provides methods to subscribe to query results in a reactive way.
 *
 * <p>Note: To be used with the acreactive framework, such as ProjectReactor or R2DBC.
 *
 * @see ResultSet
 */
public interface ReactiveResultSet extends Flow.Publisher<SqlRow> {
    /**
     * Returns a publisher for the result metadata.
     *
     * @return Metadata publisher.
     * @see ResultSet#metadata()
     */
    Flow.Publisher<@Nullable ResultSetMetadata> metadata();

    /**
     * Returns a publisher for the flag that determines whether the query result is a collection of rows.
     *
     * <p>Note: {@code false} value means that the query is either conditional or an update.
     *
     * @return HasRowSet flag Publisher.
     * @see ResultSet#hasRowSet()
     */
    Flow.Publisher<Boolean> hasRowSet();

    /**
     * Returns the number of rows affected by the DML statement execution (such as "INSERT", "UPDATE", etc.).
     * Returns {@code 0} if the statement returns nothing (such as "ALTER TABLE", etc.) or {@code -1} if not applicable.
     *
     * <p>Note: If the method returns {@code -1}, either {@link #hasRowSet()} or {@link #wasApplied()} returns {@code true}.
     *
     * @return Number of rows.
     * @see ResultSet#affectedRows()
     */
    Flow.Publisher<Long> affectedRows();
//What does "not applicable" mean here?

    /**
     * Returns a publisher for the flag that determines whether the query that produces the result was conditional.
     *
     * <p>Note: {@code false} means the query either returned rows or was an update query.
     *
     * @return AppliedFlag Publisher.
     * @see ResultSet#wasApplied()
     */
    Flow.Publisher<Boolean> wasApplied();
}
