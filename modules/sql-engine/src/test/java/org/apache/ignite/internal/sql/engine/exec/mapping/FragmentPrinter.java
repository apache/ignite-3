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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;

/**
 * Converts {@link MappedFragment} to text representation.
 */
final class FragmentPrinter extends IgniteRelShuttle {

    static String FRAGMENT_PREFIX = "Fragment#";

    private final Output output;

    private final ObjectIdCollector collector;

    private FragmentPrinter(Output output, ObjectIdCollector collector) {
        this.output = output;
        this.collector = collector;
    }

    static String fragmentsToString(List<MappedFragment> mappedFragments) {
        ObjectIdCollector collector = new ObjectIdCollector();

        for (MappedFragment mappedFragment : mappedFragments) {
            Fragment fragment = mappedFragment.fragment();
            collector.enumerateIds(fragment);
        }

        Output output = new Output(val -> {
            if (val instanceof ObjectId) {
                ObjectId objectId = (ObjectId) val;
                return objectId.format();
            } else if (val instanceof IgniteDistribution) {
                IgniteDistribution distribution = (IgniteDistribution) val;
                return formatDistribution(distribution, collector);
            }
            return String.valueOf(val);
        });

        for (MappedFragment mappedFragment : mappedFragments) {
            FragmentPrinter printer = new FragmentPrinter(output, collector);
            printer.print(mappedFragment);

            output.writeNewline();
        }

        return output.builder.toString();
    }

    void print(MappedFragment mappedFragment) {
        Fragment fragment = mappedFragment.fragment();
        ObjectId fragmentId = collector.objectIndices.get(fragment.fragmentId());

        output.setNewLinePadding(0);
        output.writeFormattedString(FRAGMENT_PREFIX + "{}", fragmentId);

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
            output.writeString("target:");

            output.setNewLinePadding(4);
            output.writeNewline();

            List<String> sortedNodeNames = target.nodeNames()
                    .stream()
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValue("nodes", sortedNodeNames.toString());
            output.writeNewline();

            List<ObjectId> sortedTargetIds = target.sourceIds().stream()
                    .map(sourceId -> {
                        ObjectId v = collector.objectIndices.get(sourceId);
                        // TODO  https://issues.apache.org/jira/browse/IGNITE-20495
                        //  This whole branch sourceId = -1 should not be necessary after this issue is fixed.
                        if (sourceId == -1) {
                            if (v != null) {
                                throw new IllegalStateException();
                            }
                            return new ObjectId(-1, -1);
                        } else {
                            if (v == null) {
                                throw new IllegalStateException(
                                        "No id for target source id=" + sourceId + ". Existing: " + collector.objectIndices);
                            }
                            return v;
                        }
                    })
                    .sorted(Comparator.comparing(a -> a.format()))
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValues("sources", sortedTargetIds);
            output.writeNewline();

            if (!target.assignments().isEmpty()) {
                List<String> sortedAssignments = target.assignments().stream()
                        .map(a -> a.name() + ":" + a.term())
                        .sorted(Comparator.naturalOrder())
                        .collect(Collectors.toList());

                output.appendPadding();
                output.writeKeyValue("assignments", sortedAssignments.toString());
                output.writeNewline();
            }
        }

        output.setNewLinePadding(2);

        if (!fragment.tables().isEmpty()) {
            List<String> tables = fragment.tables().stream()
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

        output.appendPadding();
        output.writeKeyValue("nodes", mappedFragment.nodes().toString());
        output.writeNewline();

        List<IgniteReceiver> remotes = mappedFragment.fragment().remotes();
        if (!remotes.isEmpty()) {
            List<ObjectId> remotesVals = remotes.stream()
                    .map(r -> {
                        long id = r.sourceFragmentId();
                        ObjectId objectId = collector.objectIndices.get(id);
                        assert objectId != null : "Unknown object id " + id;
                        return objectId;
                    })
                    .collect(Collectors.toList());

            output.appendPadding();
            output.writeKeyValues("remoteFragments", remotesVals);
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
        output.writeString(rel.getClass().getSimpleName());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteSender rel) {
        ObjectId targetId = collector.objectIndices.get(rel.targetFragmentId());
        ObjectId exchangeId = collector.objectIndices.get(rel.exchangeId());

        output.writeFormattedString("(targetFragment={}, exchange={}, distribution={})", targetId, exchangeId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteReceiver rel) {
        ObjectId srcId = collector.objectIndices.get(rel.sourceFragmentId());
        ObjectId exchangeId = collector.objectIndices.get(rel.exchangeId());

        output.writeFormattedString("(sourceFragment={}, exchange={}, distribution={})", srcId, exchangeId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteIndexScan rel) {
        ObjectId sourceId = collector.objectIndices.get(rel.sourceId());
        String tableName = String.join(".", rel.getTable().getQualifiedName());

        output.writeFormattedString("(name={}, source={}, distribution={})", tableName, sourceId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableScan rel) {
        ObjectId sourceId = collector.objectIndices.get(rel.sourceId());
        String tableName = String.join(".", rel.getTable().getQualifiedName());

        output.writeFormattedString("(name={}, source={}, distribution={})", tableName, sourceId, rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableModify rel) {
        String tableName = String.join(".", rel.getTable().getQualifiedName());

        // TODO  https://issues.apache.org/jira/browse/IGNITE-20495 there is no sourceId on TableModifyNode
        output.writeFormattedString("(name={}, source={}, distribution={})", tableName, "-1", rel.distribution());
        return super.visit(rel);
    }

    @Override
    public IgniteRel visit(IgniteTableFunctionScan rel) {
        output.writeFormattedString("(source={}, distribution={})", rel.sourceId(), rel.distribution());
        return super.visit(rel);
    }

    private static String formatDistribution(IgniteDistribution distribution, ObjectIdCollector collector) {
        if (distribution.function() instanceof AffinityDistribution) {
            AffinityDistribution f = (AffinityDistribution) distribution.function();
            int tableId = f.tableId();
            String tableName = collector.tableIdTableName.get(tableId);
            if (tableName == null) {
                throw new IllegalStateException("Unknown tableId: " + tableId + ". Existing: " + collector.tableIdTableName);
            }
            return format("affinity[table: {}]", tableName);
        } else {
            return distribution.toString();
        }
    }

    /**
     * Object identifier + object index assigned during tree traversal by {@link ObjectIdCollector}.
     */
    private static final class ObjectId {

        private final long id;

        private final int idx;

        ObjectId(long id, int idx) {
            this.id = id;
            this.idx = idx;
        }

        String format() {
            // TODO  https://issues.apache.org/jira/browse/IGNITE-20495 Should be removed after te issue is fixed.
            if (id == -1) {
                return "-1";
            }
            return String.valueOf(idx);
        }

        @Override
        public String toString() {
            return format() + "::" + id;
        }
    }

    /** Assigns object ids to various components of fragments mapping, so they can be consistent across test runs. */
    private static class ObjectIdCollector extends IgniteRelShuttle {

        // We are using the same index counter for both fragments and objects (see QuerySplitter::visit(IgniteExchange).
        private final Map<Long, ObjectId> objectIndices = new HashMap<>();

        private final Map<Integer, String> tableIdTableName = new HashMap<>();

        void enumerateIds(Fragment fragment) {
            addObjectId(fragment.fragmentId(), objectIndices);

            fragment.root().accept(this);
        }

        @Override
        public IgniteRel visit(IgniteIndexScan rel) {
            addObjectId(rel.sourceId(), objectIndices);

            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tableIdTableName.put(igniteTable.id(), igniteTable.name());
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteTableScan rel) {
            addObjectId(rel.sourceId(), objectIndices);

            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tableIdTableName.put(igniteTable.id(), igniteTable.name());
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteTableModify rel) {
            // TODO  https://issues.apache.org/jira/browse/IGNITE-20495 there is no sourceId on TableModifyNode
            //addObjectId(rel.getClass(), rel.sourceId(), objectIndices);

            IgniteTable igniteTable = rel.getTable().unwrap(IgniteTable.class);
            tableIdTableName.put(igniteTable.id(), igniteTable.name());
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteSender rel) {
            addObjectId(rel.exchangeId(), objectIndices);
            return super.visit(rel);
        }

        @Override
        public IgniteRel visit(IgniteReceiver rel) {
            addObjectId(rel.exchangeId(), objectIndices);
            return super.visit(rel);
        }

        private static void addObjectId(long id, Map<Long, ObjectId> map) {
            map.computeIfAbsent(id, (k) -> new ObjectId(k, map.size()));
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

            builder.append("]");
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