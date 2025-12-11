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
 * A functional interface representing a scalar SQL expression.
 *
 * <p>This interface defines a single method, {@link #get(ExecutionContext)}, 
 * which computes a value based on the given execution context.
 *
 * <p>For example, a scalar might compute a value based on the literal expression 
 * or dynamic parameter value provided in the execution context.
 *
 * @param <T> The type of the computed result.
 */
@FunctionalInterface
public interface SqlScalar<T> {
    /**
     * Computes a value based on the provided execution context.
     *
     * @param context The execution context, providing access to query-related data.
     * @param <RowT> The type of the execution row.
     * @return The computed value.
     */
    <RowT> T get(ExecutionContext<RowT> context);
}
