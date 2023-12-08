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

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jetbrains.annotations.Nullable;

/**
 * Defines a general typed expression in a Query instance. The generic type parameter is a reference to the type the expression is bound to.
 */
public interface CriteriaElement {
    /**
     * Accept the visitor with the given context.
     *
     * @param <C> context type
     * @param v visitor
     * @param context context of visit
     */
    <C> void accept(CriteriaVisitor<C> v, @Nullable C context);

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
    static <T> CriteriaElement equalTo(T value) {
        return Operation.create("= {0}", List.of(new Argument<>(value)));
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
    static <T> CriteriaElement greaterThan(T value) {
        return Operation.create("> {0}", List.of(new Argument<>(value)));
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
    static <T> CriteriaElement greaterThanOrEqualTo(T value) {
        return Operation.create(">= {0}", List.of(new Argument<>(value)));
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
    static <T> CriteriaElement lessThan(T value) {
        return Operation.create("< {0}", List.of(new Argument<>(value)));
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
    static <T> CriteriaElement lessThanOrEqualTo(T value) {
        return Operation.create("<= {0}", List.of(new Argument<>(value)));
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
    static <T> CriteriaElement nullValue() {
        return new StaticText("IS NULL");
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
    static <T> CriteriaElement notNullValue() {
        return new StaticText("IS NOT NULL");
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
    static <T> CriteriaElement in(T... values) {
        List<CriteriaElement> args = Arrays.stream(values)
                .map(Argument::new)
                .collect(toList());

        var template = IntStream.range(0, args.size())
                .mapToObj(i -> String.format("{%d}", i))
                .collect(Collectors.joining(", ", "IN (", ")"));

        return Operation.create(template, args);
    }
}
