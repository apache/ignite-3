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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a criteria query predicate.
 *
 * <pre>{@code
 *      public ClosableCursor<Product> uncategorizedProducts() {
 *         return products.recordView(Product.class).queryCriteria(null, columnValue("category", nullValue()));
 *      }
 * }</pre>
 *
 * @see CriteriaQuerySource
 */
public interface Criteria {
    /**
     * Accept the visitor with the given context.
     *
     * @param <C> Context type.
     * @param v Visitor.
     * @param context Context of visit.
     */
    <C> void accept(CriteriaVisitor<C> v, @Nullable C context);

    /**
     * Creates a predicate that tests whether the column value is equal to the given condition.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", equalTo("toys"))
     *     columnValue(IgniteNameUtils.quote("subCategory"), equalTo("puzzle"))
     * }</pre>
     *
     * @param columnName Column name must use SQL-parser style notation; e.g., <br>
     *                   "myColumn", creates a predicate for the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"", creates a predicate for the column with respect to case sensitivity.
     * @param condition Target condition.
     * @return The created expression instance.
     */
    static Expression columnValue(String columnName, Condition condition) {
        Criteria[] oldElements = condition.getElements();
        var newElements = new Criteria[oldElements.length + 1];

        newElements[0] = new Column(columnName);
        System.arraycopy(oldElements, 0, newElements, 1, oldElements.length);

        return new Expression(condition.getOperator(), newElements);
    }

    /**
     * Creates the negation of the predicate.
     *
     * <p>For example:
     * <pre>{@code
     *     not(columnValue("category", equalTo("toys")))
     * }</pre>
     *
     * @param expression Expression.
     * @return The created negation of the expression.
     */
    static Expression not(Expression expression) {
        requireNonNull(expression, "expression must not be null");

        return new Expression(Operator.NOT, expression);
    }

    /**
     * Creates the {@code and} of the expressions.
     *
     * <p>For example:
     * <pre>{@code
     *     and(columnValue("category", equalTo("toys")), columnValue("quantity", lessThan(20)))
     * }</pre>
     *
     * @param expressions Expressions.
     * @return The created {@code and} expression instance.
     */
    static Expression and(Expression... expressions) {
        if (expressions == null || expressions.length == 0 || Arrays.stream(expressions).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("expressions must not be empty or null");
        }

        return new Expression(Operator.AND, expressions);
    }

    /**
     * Creates the {@code or} of the expressions.
     *
     * <p>For example:
     * <pre>{@code
     *     or(columnValue("category", equalTo("toys")), columnValue("category", equalTo("games")))
     * }</pre>
     *
     * @param expressions Expressions.
     * @return The created {@code or} expressions instance.
     */
    static Expression or(Expression... expressions) {
        if (expressions == null || expressions.length == 0 || Arrays.stream(expressions).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("expressions must not be empty or null");
        }

        return new Expression(Operator.OR, expressions);
    }

    /**
     * Creates a condition that test the examined object is equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", equalTo("toys"))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition equalTo(Comparable<T> value) {
        if (value == null) {
            return nullValue();
        }

        return new Condition(Operator.EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("password", equalTo("MyPassword".getBytes()))
     * }</pre>
     *
     * @param value Target value.
     */
    static Condition equalTo(byte[] value) {
        if (value == null) {
            return nullValue();
        }

        return new Condition(Operator.EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is not equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", notEqualTo("toys"))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition notEqualTo(Comparable<T> value) {
        if (value == null) {
            return notNullValue();
        }

        return new Condition(Operator.NOT_EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is not equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("password", notEqualTo("MyPassword".getBytes()))
     * }</pre>
     *
     * @param value Target value.
     */
    static Condition notEqualTo(byte[] value) {
        if (value == null) {
            return notNullValue();
        }

        return new Condition(Operator.NOT_EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is greater than the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("age", greaterThan(35))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition greaterThan(Comparable<T> value) {
        return new Condition(Operator.GT, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is greater than or equal than the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("age", greaterThanOrEqualTo(35))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition greaterThanOrEqualTo(Comparable<T> value) {
        return new Condition(Operator.GOE, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is less than the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("age", lessThan(35))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition lessThan(Comparable<T> value) {
        return new Condition(Operator.LT, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is less than or equal than the specified {@code value}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("age", lessThanOrEqualTo(35))
     * }</pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition lessThanOrEqualTo(Comparable<T> value) {
        return new Condition(Operator.LOE, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is null.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", nullValue())
     * }</pre>
     */
    static Condition nullValue() {
        return new Condition(Operator.IS_NULL);
    }

    /**
     * Creates a condition that test the examined object is not null.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", notNullValue())
     * }</pre>
     */
    static Condition notNullValue() {
        return new Condition(Operator.IS_NOT_NULL);
    }

    /**
     * Creates a condition that test the examined object is is found within the specified {@code collection}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", in("toys", "games"))
     * }</pre>
     *
     * @param <T> Values type.
     * @param values The collection in which matching items must be found.
     */
    static <T> Condition in(Comparable<T>... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("values must not be empty or null");
        }

        if (values.length == 1) {
            return equalTo(values[0]);
        }

        Criteria[] args = Arrays.stream(values)
                .map(Parameter::new)
                .toArray(Criteria[]::new);

        return new Condition(Operator.IN, args);
    }

    /**
     * Creates a condition that test the examined object is is found within the specified {@code collection}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("password", in("MyPassword".getBytes(), "MyOtherPassword".getBytes()))
     * }</pre>
     *
     * @param values The collection in which matching items must be found.
     */
    static Condition in(byte[]... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("values must not be empty or null");
        }

        if (values.length == 1) {
            return equalTo(values[0]);
        }

        Criteria[] args = Arrays.stream(values)
                .map(Parameter::new)
                .toArray(Criteria[]::new);

        return new Condition(Operator.IN, args);
    }

    /**
     * Creates a condition that test the examined object is is not found within the specified {@code collection}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("category", notIn("toys", "games"))
     * }</pre>
     *
     * @param <T> Values type.
     * @param values The collection in which matching items must be not found.
     */
    static <T> Condition notIn(Comparable<T>... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("values must not be empty or null");
        }

        if (values.length == 1) {
            return notEqualTo(values[0]);
        }

        Criteria[] args = Arrays.stream(values)
                .map(Parameter::new)
                .toArray(Criteria[]::new);

        return new Condition(Operator.NOT_IN, args);
    }

    /**
     * Creates a condition that test the examined object is is not found within the specified {@code collection}.
     *
     * <p>For example:
     * <pre>{@code
     *     columnValue("password", notIn("MyPassword".getBytes(), "MyOtherPassword".getBytes()))
     * }</pre>
     *
     * @param values The collection in which matching items must be not found.
     */
    static Condition notIn(byte[]... values) {
        if (values.length == 0) {
            throw new IllegalArgumentException("values must not be empty or null");
        }

        if (values.length == 1) {
            return notEqualTo(values[0]);
        }

        Criteria[] args = Arrays.stream(values)
                .map(Parameter::new)
                .toArray(Criteria[]::new);

        return new Condition(Operator.NOT_IN, args);
    }
}
