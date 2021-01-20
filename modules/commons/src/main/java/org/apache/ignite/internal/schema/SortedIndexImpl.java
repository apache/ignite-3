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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.SortedIndexColumn;

/**
 * Sorted index.
 */
public class SortedIndexImpl extends AbstractSchemaObject implements SortedIndex {
    /** Index inline size. */
    protected final int inlineSize;

    /** Columns. */
    private final List<SortedIndexColumn> cols;

    /** Unique flag. */
    private final boolean uniq;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param cols Index columns.
     * @param inlineSize Inline size.
     * @param uniq Unique flag.
     */
    public SortedIndexImpl(String name, List<SortedIndexColumn> cols, int inlineSize, boolean uniq) {
        super(name);

        this.inlineSize = inlineSize;
        this.cols = Collections.unmodifiableList(cols);
        this.uniq = uniq;
    }

    /** {@inheritDoc} */
    public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public List<SortedIndexColumn> columns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public List<SortedIndexColumn> indexedColumns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public boolean unique() {
        return uniq;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SortedIndex[" +
            "name='" + name() + '\'' +
            ", type=SORTED" +
            ", inline=" + inlineSize +
            ", uniq=" + uniq +
            ", columns=[" + columns().stream().map(IndexColumn::toString).collect(Collectors.joining(",")) +
            "]]";
    }

}
