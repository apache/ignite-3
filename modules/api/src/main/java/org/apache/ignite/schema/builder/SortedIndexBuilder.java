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

package org.apache.ignite.schema.builder;

import java.util.Map;
import org.apache.ignite.schema.SortedIndex;

/**
 * Sorted index descriptor builder.
 */
public interface SortedIndexBuilder extends SchemaObjectBuilder {
    /**
     * Sets unique flag to {@code true}.
     *
     * @return {@code this} for chaining.
     */
    SortedIndexBuilder unique();

    /**
     * Adds a column to index.
     *
     * @param name Table column name.
     * @return Index builder.
     */
    SortedIndexColumnBuilder addIndexColumn(String name);

    /** {@inheritDoc} */
    @Override SortedIndexBuilder withHints(Map<String, String> hints);

    /**
     * Builds sorted index.
     *
     * @return Sorted index.
     */
    @Override SortedIndex build();

    /**
     * Index column builder.
     */
    @SuppressWarnings("PublicInnerClass")
    interface SortedIndexColumnBuilder {
        /**
         * Sets descending sort order.
         *
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder desc();

        /**
         * Sets ascending sort order.
         *
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder asc();

        /**
         * Sets column name.
         *
         * @param name Column name.
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder withName(String name);

        /**
         * Builds and adds the column to the index.
         *
         * @return Parent builder for chaining.
         */
        SortedIndexBuilder done();
    }
}
