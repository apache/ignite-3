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

package org.apache.ignite.internal.schema.builder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.SortedIndexImpl;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.builder.SortedIndexBuilder;

/**
 * Sorted index.
 */
public class SortedIndexBuilderImpl extends AbstractIndexBuilder implements SortedIndexBuilder {
    /** Index columns. */
    protected final Map<String, SortedIndexColumnBuilderImpl> cols = new HashMap<>();

    /** Inline size. */
    protected int inlineSize;

    /** Unique flag. */
    protected boolean uniq;

    /**
     * Constructor.
     *
     * @param name Index name.
     */
    public SortedIndexBuilderImpl(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public SortedIndexBuilderImpl withInlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public SortedIndexColumnBuilderImpl addIndexColumn(String name) {
        return new SortedIndexColumnBuilderImpl(this).withName(name);
    }

    /** {@inheritDoc} */
    @Override public SortedIndexBuilderImpl unique() {
        uniq = true;

        return this;
    }

    /**
     * @param idxBulder Index builder.
     */
    protected void addIndexColumn(SortedIndexColumnBuilderImpl idxBulder) {
        if (cols.put(idxBulder.name(), idxBulder) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + idxBulder.name());
    }

    /**
     * @return Index columns.
     */
    public Collection<? extends SortedIndexColumnBuilderImpl> columns() {
        return cols.values();
    }

    /** {@inheritDoc} */
    @Override public SortedIndex build() {
        assert !cols.isEmpty();

        return new SortedIndexImpl(
            name,
            cols.values().stream().map(c -> new SortedIndexImpl.IndexColumnImpl(c.name, c.asc)).collect(Collectors.toList()),
            inlineSize,
            uniq);
    }

    /**
     * Index column builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class SortedIndexColumnBuilderImpl implements SortedIndexColumnBuilder {
        /** Index builder. */
        private final SortedIndexBuilderImpl idxBuilder;

        /** Columns name. */
        protected String name;

        /** Index order flag. */
        protected boolean asc = true;

        /**
         * Constructor.
         *
         * @param idxBuilder Index builder.
         */
        public SortedIndexColumnBuilderImpl(SortedIndexBuilderImpl idxBuilder) {
            this.idxBuilder = idxBuilder;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl desc() {
            asc = false;

            return this;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl asc() {
            asc = true;

            return this;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexColumnBuilderImpl withName(String name) {
            this.name = name;

            return this;
        }

        public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public SortedIndexBuilderImpl done() {
            idxBuilder.addIndexColumn(this);

            return idxBuilder;
        }
    }
}
