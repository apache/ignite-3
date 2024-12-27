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
 * A functional interface representing a SQL projection operation.
 *
 * <p>This interface defines a single method, {@link #project(ExecutionContext, RowT)}, 
 * which applies a projection to a given row.
 *
 * @param <RowT> The type of the execution row.
 */
@FunctionalInterface
public interface SqlProjection<RowT> {
    /**
     * Applies a projection to the given execution row.
     *
     * @param context The execution context, providing access to query-related data.
     * @param row The current row to be projected.
     * @return The projected row, which may be a modified version of the input row or a new row.
     */
    RowT project(ExecutionContext<RowT> context, RowT row);
}
