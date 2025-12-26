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

package org.apache.ignite.internal.sql.engine.api.expressions;

import java.util.function.LongSupplier;

/**
 * Builder interface for constructing {@link EvaluationContext} instances.
 *
 * @param <RowT> the type of row data that will be processed in the evaluation context
 */
public interface ContextBuilder<RowT> {
    /** Specifies time provider to use in CURRENT_TIMESTAMP function. */
    ContextBuilder<RowT> timeProvider(LongSupplier timeProvider);

    /** Specifies row accessor to read columns of input row, if any. */
    ContextBuilder<RowT> rowAccessor(RowAccessor<RowT> rowAccessor);

    /** Returns created context. */
    EvaluationContext<RowT> build();
}
