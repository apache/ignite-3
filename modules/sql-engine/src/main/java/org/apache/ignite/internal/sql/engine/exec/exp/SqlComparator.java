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

package org.apache.ignite.internal.sql.engine.exec.exp;

import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;

/**
 * A functional interface representing a comparator for SQL rows.
 *
 * <p>This interface defines a single method, {@link #compare(ExecutionContext, Object, Object)}, 
 * which compares two rows within the given execution context. The comparison respects the sorting 
 * order defined during the creation of the comparator, allowing for flexible custom ordering 
 * logic in SQL queries or operations that require row comparison.
 */
@FunctionalInterface
public interface SqlComparator {
    /**
     * Compares two rows within the execution context.
     *
     * @param context The execution context, providing access to query-related data.
     * @param r1 The first row to be compared.
     * @param r2 The second row to be compared
     * @param <RowT> The type of the execution row..
     * @return A negative integer, zero, or a positive integer as the first row stands before
     *         or after the second one according to the sorting order defined during the
     *         comparator's creation.
     */
    <RowT> int compare(ExecutionContext<RowT> context, RowT r1, RowT r2);
}
