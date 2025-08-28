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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import org.jetbrains.annotations.Nullable;

/**
 * Adapter that provides means to convert accumulator arguments and return types.
 */
public interface AccumulatorWrapper<RowT> {

    /** Returns {@code true} if the accumulator function should be applied to distinct elements. */
    boolean isDistinct();

    /** Returns {@code true} if the accumulator function should be applied to groupset keys. */
    boolean isGrouping();

    /** Returns the accumulator function. */
    Accumulator accumulator();

    /**
     * Creates accumulator arguments from the given row. If this method returns {@code null},
     * then the accumulator function should not be applied to the given row.
     */
    Object @Nullable [] getArguments(RowT row);

    /** Converts accumulator result. */
    Object convertResult(@Nullable Object result);
}
