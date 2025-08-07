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

package org.apache.ignite.internal.storage.pagememory.index.sorted.comparator;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.type.NativeType;
import org.jetbrains.annotations.Nullable;

/**
 * Options for {@link JitComparator} generation.
 */
public class JitComparatorOptions {
    private List<CatalogColumnCollation> columnCollations;

    private List<NativeType> columnTypes;

    private List<Boolean> nullableFlags;

    private @Nullable String className;

    boolean supportPrefixes;

    private JitComparatorOptions(
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes,
            List<Boolean> nullableFlags,
            String className,
            boolean supportPrefixes
    ) {
        this.columnCollations = columnCollations;
        this.columnTypes = columnTypes;
        this.nullableFlags = nullableFlags;
        this.className = className;
        this.supportPrefixes = supportPrefixes;
    }

    public List<CatalogColumnCollation> columnCollations() {
        return columnCollations;
    }

    public List<NativeType> columnTypes() {
        return columnTypes;
    }

    public List<Boolean> nullableFlags() {
        return nullableFlags;
    }

    public @Nullable String className() {
        return className;
    }

    public boolean supportPrefixes() {
        return supportPrefixes;
    }

    /**
     * Creates a builder for {@link JitComparatorOptions}.
     *
     * @return A new builder instance.
     */
    public static JitComparatorOptionsBuilder builder() {
        return new JitComparatorOptionsBuilder();
    }

    /**
     * Builder for {@link JitComparatorOptions}.
     */
    public static class JitComparatorOptionsBuilder {
        private List<CatalogColumnCollation> columnCollations;
        private List<NativeType> columnTypes;
        private List<Boolean> nullableFlags;
        private String className;
        private boolean supportPrefixes;

        public JitComparatorOptionsBuilder columnCollations(List<CatalogColumnCollation> columnCollations) {
            this.columnCollations = columnCollations;
            return this;
        }

        public JitComparatorOptionsBuilder columnTypes(List<NativeType> columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public JitComparatorOptionsBuilder nullableFlags(List<Boolean> nullableFlags) {
            this.nullableFlags = nullableFlags;
            return this;
        }

        public JitComparatorOptionsBuilder className(String className) {
            this.className = className;
            return this;
        }

        public JitComparatorOptionsBuilder supportPrefixes(boolean supportPrefixes) {
            this.supportPrefixes = supportPrefixes;
            return this;
        }

        public JitComparatorOptions build() {
            Objects.requireNonNull(columnCollations, "columnCollations is null");
            Objects.requireNonNull(columnTypes, "columnTypes is null");
            Objects.requireNonNull(nullableFlags, "nullableFlags is null");

            if (columnCollations.size() != columnTypes.size() || columnCollations.size() != nullableFlags.size()) {
                throw new IllegalArgumentException("Column collations, types, and nullable flags must have the same size");
            }

            return new JitComparatorOptions(columnCollations, columnTypes, nullableFlags, className, supportPrefixes);
        }
    }
}
