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

package org.apache.ignite.internal.sql.engine.exec;

import org.apache.calcite.DataContext;

/**
 * Provides the contextual environment required for evaluating SQL expressions.
 *
 * <p>This context provides necessary information for evaluation context-depended functions, such as {@code CURRENT_USER}
 * or {@code CURRENT_TIMESTAMP}, as well as the means to read input rows and create new row instances when expressions produce derived
 * values.
 *
 * @param <RowT> The type of the execution row.
 */
public interface SqlEvaluationContext<RowT> extends DataContext {
    /** Returns an accessor allowing expression evaluators to retrieve column values from an input row. */
    RowAccessor<RowT> rowAccessor();

    /** Returns a factory capable of producing {@link RowFactory} instances for the {@code RowT} row representation. */
    RowFactoryFactory<RowT> rowFactoryFactory();

    /** Returns row representing correlation source by given correlation id. */
    RowT correlatedVariable(int id);
}
