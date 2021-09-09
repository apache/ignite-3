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

package org.apache.ignite.internal.schema.definition.builder;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.schema.definition.index.PrimaryKeyImpl;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.schema.definition.PrimaryKey;
import org.apache.ignite.schema.definition.builder.PrimaryKeyBuilder;

import static org.apache.ignite.schema.definition.PrimaryKey.PRIMARY_KEY_NAME;

/**
 * Primary index builder.
 */
public class PrimaryKeyBuilderImpl extends AbstractIndexBuilder implements PrimaryKeyBuilder {
    /** Index columns. */
    @IgniteToStringInclude
    private String[] columns;

    /** Affinity columns, */
    @IgniteToStringInclude
    private String[] affinityColumns;

    /**
     * Constructor.
     */
    public PrimaryKeyBuilderImpl() {
        super(PRIMARY_KEY_NAME, true);
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withColumns(String... columns) {
        this.columns = columns;

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withAffinityColumns(String... affinityColumns) {
        this.affinityColumns = affinityColumns;

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKeyBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PrimaryKey build() {
        Set<String> cols = Set.of(columns);

        Set<String> affCols;

        if (affinityColumns != null) {
            affCols = Set.of(affinityColumns);

            if (!cols.containsAll(affCols))
                throw new IllegalStateException("Schema definition error: All affinity columns must be part of key.");
        } else
            affCols = cols;

        return new PrimaryKeyImpl(cols, affCols);
    }
}
