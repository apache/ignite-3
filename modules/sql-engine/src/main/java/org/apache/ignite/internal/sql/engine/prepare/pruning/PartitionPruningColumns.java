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
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.externalize.RelJsonReader;
import org.apache.ignite.internal.sql.engine.externalize.RelJsonWriter;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataInput;
import org.apache.ignite.internal.util.io.IgniteUnsafeDataOutput;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
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
public class PartitionPruningColumns implements Serializable {

    private static final long serialVersionUID = 0;

    private final List<Int2ObjectMap<RexNode>> columns;

    /** Constructor. */
    public PartitionPruningColumns(List<Int2ObjectMap<RexNode>> columns) {
        this.columns = Collections.unmodifiableList(columns);
    }

    /** A list of column values. */
    public List<Int2ObjectMap<RexNode>> columns() {
        return columns;
    }

    /** Returns {@code true} if this columns contain correlated variables. */
    public boolean containCorrelatedVariables() {
        return columns.stream()
                .anyMatch(c -> c.values().stream().anyMatch(PartitionPruningMetadataExtractor::isCorrelatedVariable));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionPruningColumns.class, this, "columns", columns);
    }

    @SuppressWarnings("unused")
    private SerializedForm writeReplace() {
        return new SerializedForm(this);
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

    /**
     * Serialized form to serialize rex nodes.
     */
    static class SerializedForm implements Serializable {

        private static final long serialVersionUID = 0;

        private final byte[] bytes;

        private SerializedForm(PartitionPruningColumns columns) {
            try (IgniteUnsafeDataOutput output = new IgniteUnsafeDataOutput(256)) {
                output.writeInt(columns.columns().size());

                for (Int2ObjectMap<RexNode> columnMap : columns.columns()) {
                    output.writeInt(columnMap.size());

                    for (Int2ObjectMap.Entry<RexNode> columnValue : columnMap.int2ObjectEntrySet()) {
                        String exprJson = RelJsonWriter.toExprJson(columnValue.getValue());

                        output.writeInt(columnValue.getIntKey());
                        output.writeUTF(exprJson);
                    }
                }

                this.bytes = output.array();
            } catch (IOException e) {
                throw new IgniteException(Common.INTERNAL_ERR, "Unable to serialize partition pruning metadata", e);
            }
        }

        protected final Object readResolve() {
            try (IgniteUnsafeDataInput input = new IgniteUnsafeDataInput(bytes)) {
                return readColumns(input);
            } catch (IOException e) {
                throw new IgniteException(Common.INTERNAL_ERR, "Unable to deserialize partition pruning metadata", e);
            }
        }
    }

    private static PartitionPruningColumns readColumns(IgniteDataInput input) throws IOException {
        int numColumnSets = input.readInt();
        List<Int2ObjectMap<RexNode>> result = new ArrayList<>(numColumnSets);

        for (int i = 0; i < numColumnSets; i++) {
            readExpressions(input, result);
        }

        return new PartitionPruningColumns(result);
    }

    private static void readExpressions(IgniteDataInput input, List<Int2ObjectMap<RexNode>> output) throws IOException {
        int numColumns = input.readInt();
        Int2ObjectMap<RexNode> expr = new Int2ObjectOpenHashMap<>(numColumns);

        for (int i = 0; i < numColumns; i++) {
            int key = input.readInt();
            String exprJson = input.readUTF();

            RexNode rexNode = RelJsonReader.fromExprJson(exprJson);
            expr.put(key, rexNode);
        }

        output.add(expr);
    }
}
