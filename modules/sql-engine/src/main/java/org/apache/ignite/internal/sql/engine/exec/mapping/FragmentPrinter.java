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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Cloner;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Converts {@link MappedFragment} to text representation.
 */
public final class FragmentPrinter {
    private static final int ATTRIBUTES_INDENT = 2;

    static String FRAGMENT_PREFIX = "Fragment#";

    private final boolean verbose;
    private final Output output;
    private final Int2ObjectMap<IgniteTable> tables;

    private FragmentPrinter(boolean verbose, Output output, Int2ObjectMap<IgniteTable> tables) {
        this.output = output;
        this.tables = tables;
        this.verbose = verbose;
    }

    /** Wraps mapped fragments into string representation. */
    public static String fragmentsToString(boolean verbose, List<MappedFragment> mappedFragments) {
        var collector = new TableDescriptorCollector();

        for (MappedFragment mappedFragment : mappedFragments) {
            Fragment fragment = mappedFragment.fragment();
            collector.collect(fragment);
        }

        var output = new Output();

        for (MappedFragment mappedFragment : mappedFragments) {
            FragmentPrinter printer = new FragmentPrinter(verbose, output, collector.tables);
            printer.print(mappedFragment);

            output.writeNewline();
        }

        return output.builder.toString();
    }

    private static IgniteDistribution deriveDistribution(IgniteRel rel) {
        if (rel instanceof IgniteSender) {
            return ((IgniteSender) rel).sourceDistribution();
        }

        return rel.distribution();
    }

    private void print(MappedFragment mappedFragment) {
        Fragment fragment = mappedFragment.fragment();

        output.setNewLinePadding(0);
        output.writeString(FRAGMENT_PREFIX + fragment.fragmentId());

        if (fragment.rootFragment()) {
            output.writeString(" root");
        }

        if (fragment.correlated()) {
            output.writeString(" correlated");
        }

        output.setNewLinePadding(ATTRIBUTES_INDENT);
        output.writeNewline();

        output.writeKeyValue("distribution", deriveDistribution(mappedFragment.fragment().root()).label());
        output.writeKeyValue("executionNodes", mappedFragment.nodes().toString());

        if (verbose) {
            ColocationGroup target = mappedFragment.target();
            if (target != null) {
                List<String> sortedNodeNames = target.nodeNames()
                        .stream()
                        .sorted(Comparator.naturalOrder())
                        .collect(Collectors.toList());

                output.writeKeyValue("targetNodes", sortedNodeNames.toString());
            }

            Long2ObjectMap<List<String>> sourcesByExchangeId = mappedFragment.sourcesByExchangeId();
            if (sourcesByExchangeId != null) {
                output.writeKeyValue("exchangeSourceNodes", sourcesByExchangeId.long2ObjectEntrySet()
                        .stream()
                        .map(e -> Map.entry(e.getLongKey(), new TreeSet<>(e.getValue())))
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
                        .toString()
                );
            }

            Map<Long, ColocationGroup> orderedColocationGroups = new TreeMap<>(mappedFragment.groupsBySourceId());
            for (Entry<Long, ColocationGroup> entry : orderedColocationGroups.entrySet()) {
                ColocationGroup group = entry.getValue();
                appendColocationGroup(entry.getKey(), group);
            }
        }

        Int2ObjectMap<Map<String, BitSet>> tableIdToNodeNameToPartitionsMap = new Int2ObjectOpenHashMap<>();
        for (ColocationGroup group : mappedFragment.groups()) {
            for (long sourceId : group.sourceIds()) {
                IgniteTable table = mappedFragment.fragment().tables().get(sourceId);

                if (table == null) {
                    continue;
                }

                int tableId = table.id();

                for (String nodeName : group.nodeNames()) {
                    Map<String, BitSet> nodeNameToPartitionsMap = null;

                    for (PartitionWithConsistencyToken partitionsWithToken : group.partitionsWithConsistencyTokens(nodeName)) {
                        if (nodeNameToPartitionsMap == null) {
                            nodeNameToPartitionsMap = tableIdToNodeNameToPartitionsMap
                                    .computeIfAbsent(tableId, k -> new HashMap<>());
                        }

                        nodeNameToPartitionsMap.computeIfAbsent(nodeName, k -> new BitSet()).set(partitionsWithToken.partId());
                    }
                }
            }
        }

        if (!tableIdToNodeNameToPartitionsMap.isEmpty()) {
            String partitionsAsString = tableIdToNodeNameToPartitionsMap.int2ObjectEntrySet().stream()
                    .map(e -> {
                        IgniteTable table = tables.get(e.getIntKey());
                        String tableName = table == null ? "<unknown table with id=" + e.getValue() + ">" : table.name();

                        return tableName + "=" + e.getValue().entrySet().stream()
                                .map(n2p -> n2p.getKey() + "=" + n2p.getValue())
                                .collect(Collectors.joining(", ", "[", "]"));
                    })
                    .collect(Collectors.joining(", ", "[", "]"));

            output.writeKeyValue("partitions", partitionsAsString);
        }

        output.writeKeyValue("tree", "");

        output.setNewLinePadding(ATTRIBUTES_INDENT);

        IgniteRel clonedRoot = Cloner.clone(mappedFragment.fragment().root(), Commons.cluster());
        output.writeString(ExplainUtils.toString(clonedRoot, 2 * ATTRIBUTES_INDENT));
    }

    private void appendColocationGroup(long sourceId, ColocationGroup group) {
        StringBuilder sb = new StringBuilder();

        sb.append('{')
                .append("nodes=")
                .append(new TreeSet<>(group.nodeNames()))
                .append(", sourceIds=").append(new TreeSet<>(group.sourceIds()))
                .append(", assignments=");

        Map<String, String> assignments = new TreeMap<>();
        for (Int2ObjectMap.Entry<NodeWithConsistencyToken> entry : group.assignments().int2ObjectEntrySet()) {
            String assignment = entry.getValue().name() + ":" + entry.getValue().enlistmentConsistencyToken();
            assignments.put("part_" + entry.getIntKey(), assignment);
        }

        sb.append(assignments)
                .append(", partitionsWithConsistencyTokens=");

        Map<String, String> partitionWithConsistencyTokens = new TreeMap<>();
        for (String nodeName : group.nodeNames()) {
            List<PartitionWithConsistencyToken> ppPerNode = group.partitionsWithConsistencyTokens(nodeName);

            List<String> pps = ppPerNode.stream()
                    .map(p -> "part_" + p.partId() + ":" + p.enlistmentConsistencyToken())
                    .sorted()
                    .collect(Collectors.toList());

            partitionWithConsistencyTokens.put(nodeName, pps.toString());
        }
        sb.append(partitionWithConsistencyTokens)
                .append('}');

        output.writeKeyValue("colocationGroup[" + sourceId + "]", sb.toString());
    }

    /** Collects table descriptors to use them table names, column names in text output. */
    private static class TableDescriptorCollector extends IgniteRelShuttle {

        private final Int2ObjectMap<IgniteTable> tables = new Int2ObjectOpenHashMap<>();

        void collect(Fragment fragment) {
            fragment.root().accept(this);
        }

        @Override
        public IgniteRel visit(IgniteIndexScan rel) {
            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tables.put(igniteTable.id(), igniteTable);
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteTableScan rel) {
            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tables.put(igniteTable.id(), igniteTable);
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteTableModify rel) {
            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tables.put(igniteTable.id(), igniteTable);
            return super.visit(rel);
        }
    }

    private static class Output {
        private final StringBuilder builder = new StringBuilder();

        private int newLinePadding;

        private String paddingString = " ";

        private boolean blankLine;

        /** Writes string property: {@code name: value}. */
        void writeKeyValue(String name, String value) {
            appendPadding();

            builder.append(name).append(": ").append(value);

            writeNewline();
        }

        /** Writes the given. */
        void writeString(String value) {
            builder.append(value);
        }

        void setPaddingStr(String val) {
            this.paddingString = val;
        }

        void setNewLinePadding(int value) {
            newLinePadding = value;
        }

        void writeNewline() {
            blankLine = true;
            builder.append(System.lineSeparator());
        }

        private void appendPadding() {
            boolean wasBlank = blankLine;

            if (wasBlank && newLinePadding > 0) {
                builder.append(paddingString.repeat(newLinePadding));
            }

            if (wasBlank) {
                blankLine = false;
            }
        }
    }
}
