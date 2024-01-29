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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * Metadata for partition pruning. Collection of column values for a particular relation/physical operator. Example:
 * <pre>
 *     Condition: c1 = 10 AND c2 = 42 OR c1 = 77 AND c2 = 173
 *     Colocation keys: c1, c2
 *     Columns: [c1=10, c2=42], [c1=77, c2=173]
 * </pre>
 *
 * @see PartitionPruningMetadataExtractor
 */
public class PartitionPruningColumns {

    @IgniteToStringInclude
    private final List<Int2ObjectMap<RexNode>> columns;

    /** Constructor. */
    public PartitionPruningColumns(List<Int2ObjectMap<RexNode>> columns) {
        this.columns = Collections.unmodifiableList(columns);
    }

    /** A list of column values. */
    public List<Int2ObjectMap<RexNode>> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    /** Returns column values in canonical form. E.g. {@code [1=2, 0=3]} becomes {@code [0=3, 1=2]} */
    @TestOnly
    public static List<List<Map.Entry<Integer, RexNode>>> canonicalForm(PartitionPruningColumns columns) {
        return columns.columns.stream()
                .map(cols -> cols.int2ObjectEntrySet().stream().map(e -> Map.entry(e.getIntKey(), e.getValue()))
                        .sorted(Entry.comparingByKey())
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
}
