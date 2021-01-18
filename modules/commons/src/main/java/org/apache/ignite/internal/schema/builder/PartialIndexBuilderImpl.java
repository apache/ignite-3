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

import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.PartialIndexImpl;
import org.apache.ignite.internal.schema.SortedIndexImpl;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.builder.PartialIndexBuilder;

/**
 * Partial index builder.
 */
public class PartialIndexBuilderImpl extends SortedIndexBuilderImpl implements PartialIndexBuilder {
    /** Partial index expression. */
    private String expr;

    /**
     * Constructor.
     *
     * @param idxName Index name.
     */
    public PartialIndexBuilderImpl(String idxName) {
        super(idxName);
    }

    /** {@inheritDoc} */
    @Override public PartialIndexBuilderImpl withInlineSize(int inlineSize) {
        super.withInlineSize(inlineSize);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PartialIndex build() {
        assert expr != null && !expr.trim().isEmpty();

        return new PartialIndexImpl(
            name,
            cols.values().stream().map(c -> new SortedIndexImpl.IndexColumnImpl(c.name, c.asc)).collect(Collectors.toList()),
            inlineSize,
            expr
        );
    }

    /** {@inheritDoc} */
    @Override public PartialIndexBuilder withExpression(String expr) {
        this.expr = expr;

        return this;
    }

    /** {@inheritDoc} */
    @Override public PartialIndexColumnBuilderImpl addIndexColumn(String name) {
        return new PartialIndexColumnBuilderImpl(this).withName(name);
    }

    /**
     * Index column builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class PartialIndexColumnBuilderImpl extends SortedIndexColumnBuilderImpl implements PartialIndexColumnBuilder {
        /**
         * Constructor.
         *
         * @param idxBuilder Index builder.
         */
        public PartialIndexColumnBuilderImpl(PartialIndexBuilderImpl idxBuilder) {
            super(idxBuilder);
        }

        /** {@inheritDoc} */
        @Override public PartialIndexColumnBuilderImpl desc() {
            super.desc();

            return this;
        }

        /** {@inheritDoc} */
        @Override public PartialIndexColumnBuilderImpl asc() {
            super.asc();

            return this;
        }

        /** {@inheritDoc} */
        @Override public PartialIndexColumnBuilderImpl withName(String name) {
            super.withName(name);

            return this;
        }

        /** {@inheritDoc} */
        @Override public PartialIndexBuilderImpl done() {
            return (PartialIndexBuilderImpl)super.done();
        }
    }
}
