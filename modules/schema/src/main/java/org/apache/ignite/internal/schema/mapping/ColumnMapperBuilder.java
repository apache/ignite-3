/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.mapping;

import org.apache.ignite.internal.schema.Column;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Column mapper builder interface.
 */
public interface ColumnMapperBuilder {
    /**
     * Add new column.
     *
     * @param col Column descriptor.
     * @return {@code this} for chaining.
     */
    public ColumnMapperBuilder add(@NotNull Column col);

    /**
     * Remap column.
     *
     * @param from Source column index.
     * @param to Target column index.
     * @param col Target column descriptor.
     * @return {@code this} for chaining.
     */
    public ColumnMapperBuilder add(int from, int to, @Nullable Column col);

    /**
     * @return Column mapper.
     */
    ColumnMapper build();
}
