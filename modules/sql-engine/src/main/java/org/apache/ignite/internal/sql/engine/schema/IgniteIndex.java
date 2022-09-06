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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Schema object representing an Index.
 */
public class IgniteIndex {
    /**
     * Collation, or sorting order, of a column.
     */
    public enum Collation {
        ASC_NULLS_FIRST(true, true),
        ASC_NULLS_LAST(true, false),
        DESC_NULLS_FIRST(false, true),
        DESC_NULLS_LAST(false, false);

        /** Returns collation for a given specs. */
        public static Collation of(boolean asc, boolean nullsFirst) {
            return asc ? nullsFirst ? ASC_NULLS_FIRST : ASC_NULLS_LAST
                    : nullsFirst ? DESC_NULLS_FIRST : DESC_NULLS_LAST;
        }

        public final boolean asc;

        public final boolean nullsFirst;

        Collation(boolean asc, boolean nullsFirst) {
            this.asc = asc;
            this.nullsFirst = nullsFirst;
        }
    }

    /**
     * Type of the index.
     */
    public enum Type {
        HASH, SORTED
    }

    private final List<String> columns;

    private final @Nullable List<Collation> collations;

    private final Index<?> index;

    private final Type type;

    /**
     * Constructs the Index object.
     *
     * @param index A data access object to wrap.
     */
    public IgniteIndex(Index<?> index) {
        this.index = Objects.requireNonNull(index, "index");

        this.columns = index.descriptor().columns();
        this.collations = deriveCollations(index);
        this.type = index instanceof SortedIndex ? Type.SORTED : Type.HASH;
    }

    /** Returns a list of names of indexed columns. */
    public List<String> columns() {
        return columns;
    }

    /**
     * Returns a list of collations.
     *
     * <p>The size of the collations list is guaranteed to match the size of indexed columns. The i-th
     * collation is related to an i-th column.
     *
     * @return The list of collations or {@code null} if not applicable.
     */
    public @Nullable List<Collation> collations() {
        return collations;
    }

    /** Returns the name of a current index. */
    public String name() {
        return index.name();
    }

    /** Returns an object providing access to a data. */
    public Index<?> index() {
        return index;
    }

    public Type type() {
        return type;
    }

    private static @Nullable List<Collation> deriveCollations(Index<?> index) {
        if (index.descriptor() instanceof SortedIndexDescriptor) {
            SortedIndexDescriptor descriptor = (SortedIndexDescriptor) index.descriptor();

            List<Collation> orders = new ArrayList<>(descriptor.columns().size());

            for (var column : descriptor.columns()) {
                ColumnCollation collation = descriptor.collation(column);

                assert collation != null;

                orders.add(Collation.of(collation.asc(), collation.nullsFirst()));
            }

            return List.copyOf(orders);
        }

        return null;
    }
}
