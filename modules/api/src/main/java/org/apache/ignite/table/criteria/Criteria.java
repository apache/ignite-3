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

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a criteria query predicate.
 *
 * <pre><code>
 *      public ClosableCursor&lt;Product&gt; uncategorizedProducts() {
 *         return products.recordView(Product.class).queryCriteria(null, columnValue(&quot;category&quot;, nullValue()));
 *      }
 * </code></pre>
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
     * <pre>
     *     columnValue(&quot;category&quot;, equalTo(&quot;toys&quot;))
     * </pre>
     *
     * @param columnName Column name.
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
     * <pre>
     *     not(columnValue(&quot;category&quot;, equalTo(&quot;toys&quot;)))
     * </pre>
     *
     * @param expression Expression.
     * @return The created negation of the expression.
     */
    static Expression not(Expression expression) {
        return new Expression(Operator.NOT, expression);
    }

    /**
     * Creates the {@code and} of the expressions.
     *
     * <p>For example:
     * <pre>
     *     and(columnValue(&quot;category&quot;, equalTo(&quot;toys&quot;)), columnValue(&quot;quantity&quot;, lessThan(20)))
     * </pre>
     *
     * @param expressions Expressions.
     * @return The created {@code and} expression instance.
     */
    static Expression and(Expression... expressions) {
        return new Expression(Operator.AND, expressions);
    }

    /**
     * Creates the {@code or} of the expressions.
     *
     * <p>For example:
     * <pre>
     *     or(columnValue(&quot;category&quot;, equalTo(&quot;toys&quot;)), columnValue(&quot;category&quot;, equalTo(&quot;games&quot;)))
     * </pre>
     *
     * @param expressions Expressions.
     * @return The created {@code or} expressions instance.
     */
    static Expression or(Expression... expressions) {
        return new Expression(Operator.OR, expressions);
    }

    /**
     * Creates a condition that test the examined object is equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>
     *     columnValue(&quot;category&quot;, equalTo(&quot;toys&quot;))
     * </pre>
     *
     * @param <T> Value type.
     * @param value Target value.
     */
    static <T> Condition equalTo(Comparable<T> value) {
        return new Condition(Operator.EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is equal to the specified {@code value}.
     *
     * <p>For example:
     * <pre>
     *     columnValue(&quot;password&quot;, equalTo(&quot;MyPassword&quot;.getBytes()))
     * </pre>
     *
     * @param value Target value.
     */
    static Condition equalTo(byte[] value) {
        return new Condition(Operator.EQ, new Parameter<>(value));
    }

    /**
     * Creates a condition that test the examined object is greater than the specified {@code value}.
     *
     * <p>For example:
     * <pre>
     *     columnValue("age", greaterThan(35))
     * </pre>
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
     * <pre>
     *     columnValue("age", greaterThanOrEqualTo(35))
     * </pre>
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
     * <pre>
     *     columnValue("age", lessThan(35))
     * </pre>
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
     * <pre>
     *     columnValue("age", lessThanOrEqualTo(35))
     * </pre>
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
     * <pre>
     *     columnValue(&quot;category&quot;, nullValue())
     * </pre>
     */
    static Condition nullValue() {
        return new Condition(Operator.IS_NULL);
    }

    /**
     * Creates a condition that test the examined object is not null.
     *
     * <p>For example:
     * <pre>
     *     columnValue(&quot;category&quot;, notNullValue())
     * </pre>
     */
    static Condition notNullValue() {
        return new Condition(Operator.IS_NOT_NULL);
    }

    /**
     * Creates a condition that test the examined object is is found within the specified {@code collection}.
     *
     * <p>For example:
     * <pre>
     *     columnValue(&quot;category&quot;, in(&quot;toys&quot;, &quot;games&quot;))
     * </pre>
     *
     * @param <T> Values type.
     * @param values The collection in which matching items must be found.
     */
    static <T> Condition in(Comparable<T>... values) {
        Criteria[] args = Arrays.stream(values)
                .map(Parameter::new)
                .toArray(Criteria[]::new);

        return new Condition(Operator.IN, args);
    }
}
