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

package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.SortedIndex;

/**
 * Sorted index.
 */
public class SortedIndexImpl extends AbstractIndexImpl implements SortedIndex {
    /** Index inline size. */
    protected final int inlineSize;

    /** Columns. */
    private final List<IndexColumn> cols;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param cols Index columns.
     * @param inlineSize Inline size.
     */
    public SortedIndexImpl(String name, List<IndexColumn> cols, int inlineSize) {
        super(name);

        this.inlineSize = inlineSize;
        this.cols = Collections.unmodifiableList(cols);
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public Collection<IndexColumn> columns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public Collection<IndexColumn> indexedColumns() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SortedIndex[" +
            "name='" + name + '\'' +
            ", type=SORTED" +
            ", inline=" + inlineSize +
            ", columns=[" + columns().stream().map(IndexColumn::toString).collect(Collectors.joining(",")) + ']' +
            ']';
    }

    /**
     * Index column.
     */
    public static class IndexColumnImpl implements IndexColumn {
        /** Column name. */
        private final String name;

        /** Sort order. */
        private final boolean asc;

        /**
         * Constructor.
         *
         * @param name Column name.
         * @param asc Sort order flag.
         */
        public IndexColumnImpl(String name, boolean asc) {
            this.name = name;
            this.asc = asc;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean asc() {
            return asc;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Column[" +
                "name='" + name + '\'' +
                ", order=" + (asc ? "asc" : "desc") +
                ']';
        }
    }
}
