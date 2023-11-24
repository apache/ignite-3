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
 * {@code Visitor} defines a visitor signature for {@link Criteria} instances.
 *
 * @param <C> Context type.
 */
public interface CriteriaVisitor<C> {
    /**
     * Visit a Constant instance with the given context.
     *
     * @param expr expression to visit
     * @param context context of the visit or null, if not used
     */
    <T> void visit(Constant<T> expr, @Nullable C context);

    /**
     * Visit a Constant instance with the given context.
     *
     * @param expr expression to visit
     * @param context context of the visit or null, if not used
     */
    <T> void visit(StaticText expr, @Nullable C context);
}
