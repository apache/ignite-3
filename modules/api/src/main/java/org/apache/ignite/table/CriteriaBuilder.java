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

/**
 * Represents a criteria builder. Implementations of this interface are basic building blocks for performing criteria queries.
 */
public interface CriteriaBuilder extends Criteria {
    static ColumnObject columnName(String columnName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    CriteriaBuilder and(Criteria criteria);

    CriteriaBuilder or(Criteria criteria);

    /**
     * Represents a column . Implementations of this interface are basic building blocks for performing criteria queries.
     */
    interface ColumnObject {
        /**
         * Create a predicate for testing the column is equal to a given value.
         *
         * @param value Column value.
         * @return the created <b>equal</b> predicate instance.
         */
        <R> CriteriaBuilder equal(Comparable<R> value);

        /**
         * Create a predicate for testing whether the column is greater than value.
         *
         * @param value Column value.
         * @return the created <b>greaterThan</b> predicate instance.
         */
        <R> CriteriaBuilder greaterThan(Comparable<R> value);

        /**
         * Create a predicate for testing whether the column is greater than or equal to value.
         *
         * @param value Column value.
         * @return the created <b>greaterThanOrEqualTo</b> predicate instance.
         */
        <R> Criteria greaterThanOrEqualTo(Comparable<R> value);

        /**
         * Create a predicate for testing whether the column is less than value.
         *
         * @param value Column value.
         * @return the created <b>lessThan</b> predicate instance.
         */
        <R> Criteria lessThan(Comparable<R> value);

        /**
         * Create a predicate for testing whether the column is less than or equal to value.
         *
         * @param value Column value.
         * @return the created <b>lessThanOrEqualTo</b> predicate instance.
         */
        <R> Criteria lessThanOrEqualTo(Comparable<R> value);

        /**
         * Create a predicate for testing whether the first argument is an element of a certain collection.
         *
         * @param values Column values.
         * @return the created <b>in</b> predicate instance.
         */
        <R> Criteria in(String columnName, Comparable<R>... values);
    }
}
