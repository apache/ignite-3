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

package org.apache.ignite.internal.table.criteria;

import java.util.Arrays;

/**
 * Basic building blocks for performing criteria queries.
 */
public class Criterias {
    /**
     * Creates a predicate that tests whether the column value is equal to the given value.
     *
     * @param columnName Column name.
     * @param element Expression.
     * @return the created <b>equal</b> predicate instance.
     */
    public static Expression columnValue(String columnName, Matcher element) {
        var oldElements = element.getElements();
        var newElements = new CriteriaElement[oldElements.length + 1];

        newElements[0] = new Column(columnName);
        System.arraycopy(oldElements, 0, newElements, 1, oldElements.length);

        return new Expression(element.getOperator(), newElements);
    }

    /**
     * Creates the negation of the predicate.
     *
     * @param criteria Criteria.
     * @return the created <b>not</b> predicate instance.
     */
    public static <T> Expression not(Expression criteria) {
        return new Expression(Operator.NOT, criteria);
    }

    /**
     * Creates the and of the predicates.
     *
     * @param criterias Criterias.
     * @return the created <b>and</b> predicate instance.
     */
    public static <T> Expression and(Expression... criterias) {
        return new Expression(Operator.AND, criterias);
    }

    /**
     * Creates the or of the predicates.
     *
     * @param criterias Criterias.
     * @return the created <b>or</b> predicate instance.
     */
    public static <T> Expression or(Expression... criterias) {
        return new Expression(Operator.OR, criterias);
    }


    /**
     * Creates a criteria element that test the examined object is equal to the specified {@code value}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.equalTo(35))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Matcher equalTo(T value) {
        return new Matcher(Operator.EQ, new Argument<>(value));
    }

    /**
     * Creates a criteria element that test the examined object is greater than the specified {@code value}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.greaterThan(35))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Matcher greaterThan(T value) {
        return new Matcher(Operator.GT, new Argument<>(value));
    }

    /**
     * Creates a criteria element that test the examined object is greater than or equal than the specified {@code value}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.greaterThanOrEqualTo(35))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Matcher greaterThanOrEqualTo(T value) {
        return new Matcher(Operator.GOE, new Argument<>(value));
    }

    /**
     * Creates a criteria element that test the examined object is less than the specified {@code value}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.lessThan(35))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Matcher lessThan(T value) {
        return new Matcher(Operator.LT, new Argument<>(value));
    }

    /**
     * Creates a criteria element that test the examined object is less than or equal than the specified {@code value}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.lessThanOrEqualTo(35))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Matcher lessThanOrEqualTo(T value) {
        return new Matcher(Operator.LOE, new Argument<>(value));
    }

    /**
     * Creates a criteria element that test the examined object is null.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.nullValue())
     * </pre>
     *
     * @param <T> Value type.
     */
    static <T> Matcher nullValue() {
        return new Matcher(Operator.IS_NULL);
    }

    /**
     * Creates a criteria element that test the examined object is not null.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.notNullValue())
     * </pre>
     *
     * @param <T> Value type.
     */
    static <T> Matcher notNullValue() {
        return new Matcher(Operator.IS_NOT_NULL);
    }

    /**
     * Creates a criteria element that test the examined object is is found within the specified {@code collection}.
     * For example:
     * <pre>
     * columnValue("age", CriteriaElement.in(35, 36, 37))
     * </pre>
     *
     * @param <T> Value type.
     * @param values The collection in which matching items must be found.
     */
    static <T> Matcher in(T... values) {
        CriteriaElement[] args = Arrays.stream(values)
                .map(Argument::new)
                .toArray(CriteriaElement[]::new);

        return new Matcher(Operator.IN, args);
    }
}
