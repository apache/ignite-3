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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * Converts {@link MappedFragment} to text representation.
 */
final class FragmentPrinter extends IgniteRelShuttle {

    static String FRAGMENT_PREFIX = "Fragment#";

    private final Output output;

    private FragmentPrinter(Output output) {
        this.output = output;
    }

    static String fragmentsToString(List<MappedFragment> mappedFragments) {
        TableDescriptorCollector collector = new TableDescriptorCollector();

        for (MappedFragment mappedFragment : mappedFragments) {
            Fragment fragment = mappedFragment.fragment();
            collector.collect(fragment);
        }

        Output output = new Output(val -> {
            if (val instanceof IgniteDistribution) {
                IgniteDistribution distribution = (IgniteDistribution) val;
                return formatDistribution(distribution, collector);
            } else {
                return String.valueOf(val);
            }
        });

        for (MappedFragment mappedFragment : mappedFragments) {
            FragmentPrinter printer = new FragmentPrinter(output);
            printer.print(mappedFragment);

            output.writeNewline();
        }

        return output.builder.toString();
    }

    void print(MappedFragment mappedFragment) {
        Fragment fragment = mappedFragment.fragment();

        output.setNewLinePadding(0);
        output.writeFormattedString(FRAGMENT_PREFIX + "{}", fragment.fragmentId());

        if (fragment.rootFragment()) {
            output.writeString(" root");
        }

        if (fragment.correlated()) {
            output.writeString(" correlated");
        }

        output.appendPadding();
        output.setNewLinePadding(2);
        output.writeNewline();

        ColocationGroup target = mappedFragment.target();
        if (target != null) {
            output.appendPadding();
            List<String> sortedNodeNames = target.nodeNames()
                    .stream()
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());

            output.writeKeyValue("targetNodes", sortedNodeNames.toString());
            output.writeNewline();
        }

        output.setNewLinePadding(2);

        output.appendPadding();
        output.writeKeyValue("executionNodes", mappedFragment.nodes().toString());
        output.writeNewline();

        List<IgniteReceiver> remotes = mappedFragment.fragment().remotes();
        if (!remotes.isEmpty()) {
            List<Long> remotesVals = remotes.stream()
                    .map(IgniteReceiver::sourceFragmentId)
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValues("remoteFragments", remotesVals);
            output.writeNewline();
        }

        Long2ObjectMap<List<String>> sourcesByExchangeId = mappedFragment.sourcesByExchangeId();
        if (sourcesByExchangeId != null) {
            output.appendPadding();
            output.writeKeyValue("exchangeSourceNodes", sourcesByExchangeId.long2ObjectEntrySet()
                    .stream()
                    .map(e -> Map.entry(e.getLongKey(), new TreeSet<>(e.getValue())))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
                    .toString()
            );
            output.writeNewline();
        }

        if (!fragment.tables().isEmpty()) {
            List<String> tables = fragment.tables().values().stream()
                    .map(IgniteDataSource::name)
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValue("tables", tables.toString());
            output.writeNewline();
        }

        if (!fragment.systemViews().isEmpty()) {
            List<String> tables = fragment.systemViews().stream()
                    .map(IgniteDataSource::name)
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValue("systemViews", tables.toString());
            output.writeNewline();
        }

        Map<String, List<PartitionWithConsistencyToken>> partitions = new HashMap<>();
        for (ColocationGroup group : mappedFragment.groups()) {
            Comparator<PartitionWithConsistencyToken> cmp = Comparator.comparing(PartitionWithConsistencyToken::partId)
                    .thenComparing(PartitionWithConsistencyToken::enlistmentConsistencyToken);

            for (String nodeName : mappedFragment.nodes()) {
                List<PartitionWithConsistencyToken> nodePartitions = group.partitionsWithConsistencyTokens(nodeName);
                if (!nodePartitions.isEmpty()) {
                    nodePartitions.sort(cmp);
                    partitions.put(nodeName, nodePartitions);
                }
            }
        }

        if (!partitions.isEmpty()) {
            String partitionsAsString = partitions.entrySet().stream()
                    .map(e -> {
                        String valuesStr = e.getValue().stream()
                                .map(v -> v.partId() + ":" + v.enlistmentConsistencyToken())
                                .collect(Collectors.joining(", ", "[", "]"));

                        return e.getKey() + "=" + valuesStr;
                    })
                    .collect(Collectors.joining(", ", "{", "}"));

            output.appendPadding();
            output.writeKeyValue("partitions", partitionsAsString);
            output.writeNewline();
        }

        PartitionPruningMetadata pruningMetadata = mappedFragment.partitionPruningMetadata();
        if (pruningMetadata != null) {
            output.appendPadding();
            output.writeKeyValue("pruningMetadata", pruningMetadata.data().long2ObjectEntrySet()
                    .stream()
                    .map(e -> {
                        List<Map<Integer, RexNode>> columns = e.getValue().columns().stream()
                                .map(TreeMap::new)
                                .collect(Collectors.toList());

                        return Map.entry(e.getLongKey(), columns);
                    })
                    .sorted(Entry.comparingByKey())
                    .collect(Collectors.toList())
                    .toString()
            );
            output.writeNewline();
        }

        output.appendPadding();
        output.writeString("tree:");
        output.writeNewline();

        printRel(fragment.root());
    }

    private void printRel(IgniteRel rel) {
        String prevPaddingStr = output.paddingString;
        int prevPadding = output.newLinePadding;

        output.setPaddingStr("  ");
        output.setNewLinePadding(2);
        try {
            visit(rel);
        } finally {
            output.setNewLinePadding(prevPadding);
        }

        output.setPaddingStr(prevPaddingStr);
        output.setNewLinePadding(prevPadding);
    }

    @Override
    protected IgniteRel processNode(IgniteRel rel) {
        int prevPadding = output.newLinePadding;
        output.setNewLinePadding(output.newLinePadding + 1);
        output.writeNewline();
        try {
            return super.processNode(rel);
        } finally {
            output.setNewLinePadding(prevPadding);
        }
    }

    @Override
    public IgniteRel visit(IgniteRel rel) {
        output.appendPadding();
        output.writeString(rel.getRelTypeName());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteSender rel) {
        long targetId = rel.targetFragmentId();
        long exchangeId = rel.exchangeId();

        output.writeFormattedString("(targetFragment={}, exchange={}, distribution={})", targetId, exchangeId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteReceiver rel) {
        long sourceFragmentId = rel.sourceFragmentId();
        long exchangeId = rel.exchangeId();

        output.writeFormattedString("(sourceFragment={}, exchange={}, distribution={})", sourceFragmentId, exchangeId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        long sourceId = rel.sourceId();
        String tableName = String.join(".", rel.getTable().getQualifiedName());
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null;

        output.writeFormattedString("(name={}, source={}, partitions={}, distribution={})",
                tableName, sourceId, table.partitions(), rel.distribution()
        );
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        long sourceId = rel.sourceId();
        String tableName = String.join(".", rel.getTable().getQualifiedName());
        IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
        assert table != null;

        output.writeFormattedString("(name={}, source={}, partitions={}, distribution={})",
                tableName, sourceId, table.partitions(), rel.distribution()
        );
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableModify rel) {
        long sourceId = rel.sourceId();
        String tableName = String.join(".", rel.getTable().getQualifiedName());

        output.writeFormattedString("(name={}, source={}, distribution={})", tableName, sourceId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableFunctionScan rel) {
        output.writeFormattedString("(source={}, distribution={})", rel.sourceId(), rel.distribution());
        return super.visit(rel);
    }

    private static String formatDistribution(IgniteDistribution distribution, TableDescriptorCollector collector) {
        if (distribution.function() instanceof AffinityDistribution) {
            AffinityDistribution f = (AffinityDistribution) distribution.function();
            IgniteTable igniteTable = collector.tables.get(f.tableId());

            if (igniteTable == null) {
                String error = format("Unknown tableId: {}. Existing: {}", collector.tables.keySet());
                throw new IllegalStateException(error);
            }

            TableDescriptor tableDescriptor = igniteTable.descriptor();
            String colocationColumns = igniteTable.distribution().getKeys().stream()
                    .map(k -> tableDescriptor.columnDescriptor(k).name())
                    .collect(Collectors.joining(",", "[", "]"));

            return format("affinity[table: {}, columns: {}]", igniteTable.name(), colocationColumns);
        } else {
            return distribution.toString();
        }
    }

    /** Collects table descriptors to use them table names, column names in text output. */
    private static class TableDescriptorCollector extends IgniteRelShuttle {

        private final Map<Integer, IgniteTable> tables = new HashMap<>();

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

        private final Function<Object, String> formatter;

        private Output(Function<Object, String> formatter) {
            this.formatter = formatter;
        }

        /** Writes a list of objectIds: {@code name: [id1, id2, ..., idN]}. */
        void writeKeyValues(String name, List<?> values) {
            builder.append(name);
            builder.append(": [");

            for (Object value : values) {
                builder.append(formatter.apply(value));
                builder.append(", ");
            }
            builder.setLength(builder.length() - 2);

            builder.append(']');
        }

        /** Writes string property: {@code name: value}. */
        void writeKeyValue(String name, String value) {
            builder.append(name);
            builder.append(": ");
            builder.append(value);
        }

        /** Writes the given. */
        void writeString(String value) {
            builder.append(value);
        }

        /** Writes a formatted string. */
        void writeFormattedString(String value, Object... params) {
            List<String> formattedParams = new ArrayList<>();
            for (Object param : params) {
                formattedParams.add(formatter.apply(param));
            }

            builder.append(format(value, formattedParams.toArray()));
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
