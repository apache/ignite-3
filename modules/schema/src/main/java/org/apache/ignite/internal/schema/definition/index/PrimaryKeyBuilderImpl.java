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

package org.apache.ignite.internal.schema.definition.index;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.schema.definition.index.IndexColumn;
import org.apache.ignite.schema.definition.index.PrimaryIndex;
import org.apache.ignite.schema.definition.index.PrimaryKeyBuilder;

import static org.apache.ignite.schema.definition.index.PrimaryIndex.PRIMARY_KEY_INDEX_NAME;

/**
 * Primary index builder.
 */
public class PrimaryKeyBuilderImpl extends AbstractIndexBuilder implements PrimaryKeyBuilder {
    /** Index columns. */
    @IgniteToStringInclude
    private String[] columns;

    /** Affinity columns, */
    @IgniteToStringInclude
    private String[] affCols;

    /**
     * Constructor.
     */
    public PrimaryKeyBuilderImpl() {
        super(PRIMARY_KEY_INDEX_NAME, true);
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withColumns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withAffinityColumns(String... affCols) {
        this.affCols = affCols;

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryIndex build() {
        if (affCols == null)
            affCols = columns;

        List<IndexColumn> cols = Arrays.stream(columns).map(IndexColumnImpl::new).collect(Collectors.toList());
        List<String> affCols = Arrays.asList(this.affCols);

        if (!Set.of(cols).containsAll(affCols))
            throw new IgniteException("Schema definition error: All affinity columns must be part of key.");

        return new PrimaryKeyImpl(cols, affCols);
    }
}
