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
 * {@code Visitor} defines a visitor signature for {@link CriteriaElement} instances.
 *
 * @param <C> Context type.
 */
public interface CriteriaVisitor<C> {
    /**
     * Visit a {@link Argument} instance with the given context.
     *
     * @param argument Argument to visit
     * @param context context of the visit or null, if not used
     */
    <T> void visit(Argument<T> argument, @Nullable C context);

    /**
     * Visit a {@link StaticText} instance with the given context.
     *
     * @param text Text to visit
     * @param context context of the visit or null, if not used
     */
    <T> void visit(StaticText text, @Nullable C context);
}
