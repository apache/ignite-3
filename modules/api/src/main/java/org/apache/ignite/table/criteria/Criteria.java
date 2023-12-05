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

package org.apache.ignite.table.criteria;

import java.util.List;

/**
 * Represents a predicate. Implementations of this interface are basic building blocks for performing criteria queries.
 */
public interface Criteria extends Expression {
    /**
     * Creates a predicate that tests whether the column value is equal to the given value.
     *
     * @param columnName Column name.
     * @param expression Expression.
     * @return the created <b>equal</b> predicate instance.
     */
    static Criteria columnValue(String columnName, Expression expression) {
        return Operation.create("{0} = {1}", List.of(new StaticText(columnName), expression));
    }

    /**
     * Creates the negation of the predicate.
     *
     * @param expression Expression.
     * @return the created <b>not</b> predicate instance.
     */
    static <T> Criteria not(Expression expression) {
        return Operation.create("not {0}", List.of(expression));
    }
}
