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
        return new Argument<>(value);
    }
}
