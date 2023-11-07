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

package org.apache.ignite.sql;

/**
 * Represents a predicate. Implementations of this interface are basic building blocks for performing scan queries.
 */
public class Criteria {
    /**
     * Create a predicate for testing the field value.
     *
     * @param fieldName Field name.
     * @param value Field value.
     * @return the created <b>equal</b> predicate instance.
     */
    public static <R> Criteria equal(String fieldName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the first argument is greater than value.
     *
     * @param fieldName Field name.
     * @param value Field value.
     * @return the created <b>greaterThan</b> predicate instance.
     */
    public static <R> Criteria greaterThan(String fieldName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the first argument is greater than or equal to value.
     *
     * @param fieldName Field name.
     * @param value Field value.
     * @return the created <b>greaterThanOrEqualTo</b> predicate instance.
     */
    public static <R> Criteria greaterThanOrEqualTo(String fieldName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the first argument is less than value.
     *
     * @param fieldName Field name.
     * @param value Field value.
     * @return the created <b>lessThan</b> predicate instance.
     */
    public static <R> Criteria lessThan(String fieldName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Create a predicate for testing whether the first argument is less than or equal to value.
     *
     * @param fieldName Field name.
     * @param value Field value.
     * @return the created <b>lessThanOrEqualTo</b> predicate instance.
     */
    public static <R> Criteria lessThanOrEqualTo(String fieldName, Comparable<R> value) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates an <b>and</b> predicate that will perform the logical <b>and</b> operation on the given {@code predicates}.
     *
     * @param criterions the child predicates to form the resulting <b>and</b> predicate from.
     * @return the created <b>and</b> predicate instance.
     */
    public static Criteria and(Criteria... criterions) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates an <b>or</b> predicate that will perform the logical <b>or</b> operation on the given {@code predicates}.
     *
     * @param criterions the child predicates to form the resulting <b>or</b> predicate from.
     * @return the created <b>or</b> predicate instance.
     */
    public static Criteria or(Criteria... criterions) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Creates a <b>not</b> predicate that will negate the result of the given {@code predicate}.
     *
     * @param criteria the predicate to negate the value of.
     * @return the created <b>not</b> predicate instance.
     */
    public static Criteria not(Criteria criteria) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
