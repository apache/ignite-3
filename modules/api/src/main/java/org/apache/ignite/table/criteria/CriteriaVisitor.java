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
 * A visitor to traverse an criteria tree.
 *
 * @param <C> Context type.
 */
public interface CriteriaVisitor<C> {
    /**
     * Visit a {@link Parameter} instance with the given context.
     *
     * @param argument Parameter to visit
     * @param context context of the visit or {@code null}, if not used
     */
    <T> void visit(Parameter<T> argument, @Nullable C context);

    /**
     * Visit a {@link Column} instance with the given context.
     *
     * @param column Column to visit
     * @param context context of the visit or {@code null}, if not used
     */
    <T> void visit(Column column, @Nullable C context);

    /**
     * Visit a {@link Expression} instance with the given context.
     *
     * @param expression Expression to visit
     * @param context context of the visit or {@code null}, if not used
     */
    <T> void visit(Expression expression, @Nullable C context);

    /**
     * Visit a {@link Partition} instance with the given context.
     *
     * @param partition Partition to visit.
     * @param context context of the visit or {@code null}, if not used.
     */
    void visit(Partition partition, @Nullable C context);

    /**
     * Visit a {@link Criteria} instance with the given context.
     *
     * @param criteria Criteria to visit
     * @param context context of the visit or {@code null}, if not used
     */
    <T> void visit(Criteria criteria, @Nullable C context);
}
