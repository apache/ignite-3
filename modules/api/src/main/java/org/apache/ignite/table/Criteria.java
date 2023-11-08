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

package org.apache.ignite.table;

import org.jetbrains.annotations.Nullable;

/**
 * Represents a predicate. Implementations of this interface are basic building blocks for performing criteria queries.
 */
public interface Criteria {
    /**
     * Create a predicate for testing the column is equal to a given value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>equal</b> predicate instance.
     */
    static <R> Criteria equal(String columnName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the column is greater than value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>greaterThan</b> predicate instance.
     */
    static <R> Criteria greaterThan(String columnName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the column is greater than or equal to value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>greaterThanOrEqualTo</b> predicate instance.
     */
    static <R> Criteria greaterThanOrEqualTo(String columnName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the column is less than value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>lessThan</b> predicate instance.
     */
    static <R> Criteria lessThan(String columnName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the column is less than or equal to value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>lessThanOrEqualTo</b> predicate instance.
     */
    static <R> Criteria lessThanOrEqualTo(String columnName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the first argument is an element of a certain collection.
     *
     * @param columnName Column name.
     * @param values Column values.
     * @return the created <b>in</b> predicate instance.
     */
    static <R> Criteria in(String columnName, Comparable<R>... values) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a predicate that will perform the logical <b>and</b> operation on the given {@code predicates}.
     *
     * @param criterions the child predicates to form the resulting <b>and</b> predicate from.
     * @return the created <b>and</b> predicate instance.
     */
    static Criteria and(Criteria... criterions) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a predicate that will perform the logical <b>or</b> operation on the given {@code predicates}.
     *
     * @param criterions the child predicates to form the resulting <b>or</b> predicate from.
     * @return the created <b>or</b> predicate instance.
     */
    static Criteria or(Criteria... criterions) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a predicate that will negate the result of the given {@code predicate}.
     *
     * @param criteria the predicate to negate the value of.
     * @return the created <b>not</b> predicate instance.
     */
    static Criteria not(Criteria criteria) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a predicate that will add SQL to where clause.
     *
     * @param sql Regular SQL where clause.
     * @param arguments Arguments for the statement.
     * @return the created <b>sql</b> predicate instance.
     */
    static Criteria sql(String sql, @Nullable Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
