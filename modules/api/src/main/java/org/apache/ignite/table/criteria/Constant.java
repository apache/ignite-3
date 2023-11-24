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

import org.jetbrains.annotations.Nullable;

/**
 * {@code Constant} represents a general constant expression.
 *
 * @param <T> constant type
 */
public class Constant<T> implements Expression {
    private final T constant;

    /**
     * Create a new Constant of the given type for the given object.
     *
     * @param constant wrapped constant.
     */
    Constant(T constant) {
        this.constant = constant;
    }

    /**
     * Get the wrapped constant.
     *
     * @return wrapped constant
     */
    public T getConstant() {
        return constant;
    }

    /** {@inheritDoc} */
    @Override
    public <R, C> void accept(CriteriaVisitor<C> v, @Nullable C context) {
        v.visit(this, context);
    }
}
