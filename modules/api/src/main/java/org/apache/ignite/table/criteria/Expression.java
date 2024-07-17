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
 * Defines a expression for a criteria query with operator and it's arguments.
 *
 * @see Criteria
 */
public final class Expression implements Criteria {
    private final Operator operator;

    private final Criteria[] elements;

    /**
     * Constructor.
     *
     * @param operator Operator.
     * @param elements Criteria elements.
     */
    Expression(Operator operator, Criteria... elements) {
        this.operator = operator;
        this.elements = elements;
    }

    /**
     * Get a operator.
     *
     * @return A operator.
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Get a condition elements.
     *
     * @return A condition elements.
     */
    public Criteria[] getElements() {
        return elements;
    }

    /** {@inheritDoc} */
    @Override
    public <C> void accept(CriteriaVisitor<C> v, @Nullable C context) {
        v.visit(this, context);
    }
}
