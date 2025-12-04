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

package org.apache.ignite.internal.sql.engine.rel.explain;

import static org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils.inputRefRewriter;

import it.unimi.dsi.fastutil.objects.ObjectIntPair;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.table.QualifiedNameHelper;
import org.jetbrains.annotations.Nullable;

class RelTreeToTextWriter {
    static final int NEXT_OPERATOR_INDENT = 2;
    private static final int OPERATOR_ATTRIBUTES_INDENT = 2 * NEXT_OPERATOR_INDENT;

    private static final String OPEN_TUPLE_SYMBOL = "<";
    private static final String CLOSE_TUPLE_SYMBOL = ">";

    private static boolean needToAddFieldNames(IgniteRel rel) {
        List<RelNode> inputs = rel.getInputs();

        // Current rel is leaf node of the plan, therefore it's data source.
        // Every data source introduces their own row type, so let's include field names.
        if (inputs.isEmpty()) {
            return true;
        }

        // Set operations doesn't change the row.
        if (rel instanceof SetOp) {
            return false;
        }

        // In case of rel with single input we can do fast check.
        // Reference comparison is correct because typeFactory must intern every type.
        if (inputs.size() == 1 && rel.getRowType() == inputs.get(0).getRowType()) {
            // We want to include field names only if there were changes in number of columns, their order, or their names.
            // Therefore if row types are not equal we should double check that changes were not [only] in the column types.
            return false;
        }

        assert inputs.size() == 1 || inputs.size() == 2;

        List<String> inputNames = new ArrayList<>(inputs.get(0).getRowType().getFieldNames());

        // Join might do rename when both side contains the column with the same name.
        if (inputs.size() == 2) {
            inputNames.addAll(inputs.get(1).getRowType().getFieldNames());
        }

        return !rel.getRowType().getFieldNames().equals(inputNames);
    }

    private static RelInfoHolder collectRelInfo(IgniteRel rel) {
        RelInfoHolder infoHolder = new RelInfoHolder(rel);

        rel.explain(infoHolder);

        if (!infoHolder.attributes.containsKey(AttributeName.FIELD_NAMES) && needToAddFieldNames(rel)) {
            infoHolder.attributes.put(AttributeName.FIELD_NAMES, rel.getRowType().getFieldNames().toString());
        }

        rel.getInputs().forEach(input -> infoHolder.addChild((IgniteRel) input));

        return infoHolder;
    }

    private enum AttributeName {
        TABLE("table"),
        INDEX_NAME("index"),
        INDEX_TYPE("type"),
        PREDICATE("predicate"),
        SEARCH_BOUNDS("searchBounds"),
        FIELD_NAMES("fieldNames"),
        PROJECTION("projection"),
        COLLATION("collation"),
        TUPLES("tuples"),
        JOIN_TYPE("type"),
        DISTRIBUTION("distribution"),
        SOURCE_EXPRESSIONS("sourceExpression"),
        MODIFY_OPERATION_TYPE("type"),
        GROUP("group"),
        GROUP_SETS("groupSets"),
        AGGREGATION("aggregation"),
        KEY_EXPRESSIONS("key"),
        CORRELATED_VARIABLES("correlates"),
        INVOCATION("invocation"),
        OFFSET("offset"),
        FETCH("fetch"),
        ALL("all"),
        SOURCE_FRAGMENT_ID("sourceFragmentId"),
        TARGET_FRAGMENT_ID("targetFragmentId");

        private final String label;

        AttributeName(String label) {
            this.label = label;
        }
    }

    private static class RelInfoHolder implements IgniteRelWriter {
        private final IgniteRel rel;

        private final EnumMap<AttributeName, String> attributes = new EnumMap<>(AttributeName.class);
        private final List<RelInfoHolder> inputs = new ArrayList<>();

        RelInfoHolder(IgniteRel rel) {
            this.rel = rel;
        }

        @Override
        public IgniteRelWriter addChild(IgniteRel rel) {
            RelInfoHolder infoHolder = collectRelInfo(rel);

            inputs.add(infoHolder);

            return this;
        }

        @Override
        public IgniteRelWriter addProjection(List<RexNode> projections, RelDataType rowType) {
            RexShuttle inputRefRewriter = inputRefRewriter(rowType);

            Function<RexNode, RexNode> f = in -> in.accept(inputRefRewriter);

            attributes.put(AttributeName.PROJECTION, Commons.transform(projections, f).toString());

            return this;
        }

        @Override
        public IgniteRelWriter addPredicate(RexNode condition, RelDataType rowType) {
            attributes.put(AttributeName.PREDICATE, condition.accept(inputRefRewriter(rowType)).toString());

            return this;
        }

        @Override
        public IgniteRelWriter addTable(RelOptTable table) {
            List<String> parts = table.getQualifiedName();

            assert parts.size() == 2 : parts;

            attributes.put(AttributeName.TABLE, QualifiedNameHelper.fromNormalized(parts.get(0), parts.get(1)).toCanonicalForm());

            return this;
        }

        @Override
        public IgniteRelWriter addIndex(String name, IgniteIndex.Type type) {
            attributes.put(AttributeName.INDEX_NAME, IgniteNameUtils.quoteIfNeeded(name));
            attributes.put(AttributeName.INDEX_TYPE, type.name());

            return this;
        }

        @Override
        public IgniteRelWriter addCollation(RelCollation collation, RelDataType rowType) {
            attributes.put(AttributeName.COLLATION, beautifyCollation(collation, rowType).toString());

            return this;
        }

        @Override
        public IgniteRelWriter addTuples(List<List<RexLiteral>> tuples) {
            attributes.put(AttributeName.TUPLES, tuples.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addJoinType(JoinRelType joinType) {
            attributes.put(AttributeName.JOIN_TYPE, joinType.lowerName);

            return this;
        }

        @Override
        public IgniteRelWriter addDistribution(IgniteDistribution distribution, RelDataType rowType) {
            attributes.put(AttributeName.DISTRIBUTION, beautifyDistribution(distribution, rowType));

            return this;
        }

        @Override
        public IgniteRelWriter addSourceExpressions(List<RexNode> expressions) {
            attributes.put(AttributeName.SOURCE_EXPRESSIONS, expressions.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addModifyOperationType(TableModify.Operation operation) {
            attributes.put(AttributeName.MODIFY_OPERATION_TYPE, operation.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addGroup(ImmutableBitSet groupSet, RelDataType rowType) {
            attributes.put(AttributeName.GROUP, beautifyBitSet(groupSet, rowType).toString());

            return this;
        }

        @Override
        public IgniteRelWriter addGroupSets(List<ImmutableBitSet> groupSets, RelDataType rowType) {
            List<List<String>> sets = groupSets.stream()
                    .map(groupSet -> beautifyBitSet(groupSet, rowType))
                    .collect(Collectors.toList());

            attributes.put(AttributeName.GROUP_SETS, sets.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addAggregation(List<AggregateCall> aggCalls, RelDataType rowType) {
            RexShuttle inputRefRewriter = inputRefRewriter(rowType);

            List<String> calls = aggCalls.stream()
                    .map(call -> beautifyAggCall(call, inputRefRewriter, rowType))
                    .collect(Collectors.toList());

            attributes.put(AttributeName.AGGREGATION, calls.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addKeyExpression(List<RexNode> expressions) {
            attributes.put(AttributeName.KEY_EXPRESSIONS, expressions.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addCorrelatedVariables(Set<CorrelationId> variablesSet) {
            attributes.put(AttributeName.CORRELATED_VARIABLES, variablesSet.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addSearchBounds(List<SearchBounds> searchBounds) {
            attributes.put(AttributeName.SEARCH_BOUNDS, beautifySearchBounds(searchBounds));

            return this;
        }

        @Override
        public IgniteRelWriter addInvocation(RexNode call) {
            attributes.put(AttributeName.INVOCATION, call.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addOffset(RexNode offset) {
            attributes.put(AttributeName.OFFSET, offset.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addFetch(RexNode fetch) {
            attributes.put(AttributeName.FETCH, fetch.toString());

            return this;
        }

        @Override
        public IgniteRelWriter addAll(boolean all) {
            attributes.put(AttributeName.ALL, String.valueOf(all));

            return this;
        }

        @Override
        public IgniteRelWriter addSourceFragmentId(long fragmentId) {
            attributes.put(AttributeName.SOURCE_FRAGMENT_ID, String.valueOf(fragmentId));

            return this;
        }

        @Override
        public IgniteRelWriter addTargetFragmentId(long fragmentId) {
            attributes.put(AttributeName.TARGET_FRAGMENT_ID, String.valueOf(fragmentId));

            return this;
        }
    }

    private static String beautifySearchBounds(List<SearchBounds> searchBounds) {
        return searchBounds.stream()
                .filter(Objects::nonNull)
                .flatMap(bounds -> {
                    if (bounds.type() == SearchBounds.Type.MULTI) {
                        return ((MultiBounds) bounds).bounds().stream().filter(Objects::nonNull);
                    }
                    return Stream.of(bounds);
                })
                .map(RelTreeToTextWriter::beautifySearchBounds)
                .collect(Collectors.joining(", "));
    }

    private static String beautifySearchBounds(SearchBounds searchBounds) {
        switch (searchBounds.type()) {
            case EXACT:
                RexNode lookupKey = ((ExactBounds) searchBounds).bound();

                return OPEN_TUPLE_SYMBOL + lookupKey + CLOSE_TUPLE_SYMBOL;
            case RANGE:
                RangeBounds rangeBounds = (RangeBounds) searchBounds;

                RexNode lowerBound = rangeBounds.lowerBound();
                RexNode upperBound = rangeBounds.upperBound();

                StringBuilder sb = new StringBuilder();
                sb.append((lowerBound == null || rangeBounds.lowerInclude()) ? "[" : "(");
                if (lowerBound != null) {
                    sb.append(beautifyConditionalBound(lowerBound, rangeBounds.shouldComputeLower()));
                }
                sb.append("..");
                if (upperBound != null) {
                    sb.append(beautifyConditionalBound(upperBound, rangeBounds.shouldComputeUpper()));
                }
                sb.append((upperBound == null || rangeBounds.upperInclude()) ? "]" : ")");
                return sb.toString();
            case MULTI:
            default:
                assert false;

                return searchBounds.toString();
        }
    }

    private static String beautifyConditionalBound(RexNode bound, @Nullable RexNode condition) {
        if (condition != null && !condition.isAlwaysTrue()) {
            return OPEN_TUPLE_SYMBOL + condition + " ? " + bound + " : inf" + CLOSE_TUPLE_SYMBOL;
        } else {
            return OPEN_TUPLE_SYMBOL + bound + CLOSE_TUPLE_SYMBOL;
        }
    }

    static String dumpTree(IgniteRel rootRel, int initialLevel) {
        RelInfoHolder root = collectRelInfo(rootRel);

        Deque<ObjectIntPair<RelInfoHolder>> explanationStack = new ArrayDeque<>();
        explanationStack.add(ObjectIntPair.of(root, initialLevel));

        StringBuilder sb = new StringBuilder();
        Spacer spacer = new Spacer();
        while (!explanationStack.isEmpty()) {
            ObjectIntPair<RelInfoHolder> infoHolderWithLevel = explanationStack.pollLast();

            RelInfoHolder infoHolder = infoHolderWithLevel.first();
            int level = infoHolderWithLevel.secondInt();

            for (int i = infoHolder.inputs.size() - 1; i >= 0; i--) {
                explanationStack.add(ObjectIntPair.of(infoHolder.inputs.get(i), level + 1));
            }

            spacer.set(level * NEXT_OPERATOR_INDENT);

            spacer.spaces(sb).append(infoHolder.rel.getRelTypeName());

            spacer.add(OPERATOR_ATTRIBUTES_INDENT);

            for (AttributeName name : AttributeName.values()) {
                String attributeValue = infoHolder.attributes.get(name);

                if (attributeValue == null) {
                    continue;
                }

                sb.append(System.lineSeparator());

                spacer.spaces(sb);

                sb.append(name.label).append(": ").append(attributeValue);
            }

            sb.append(System.lineSeparator());
            spacer.spaces(sb);

            RelMetadataQuery mq = infoHolder.rel.getCluster().getMetadataQuery();
            sb.append("est: (rows=").append(
                    BigDecimal.valueOf(mq.getRowCount(infoHolder.rel)).setScale(0, RoundingMode.HALF_UP)
            ).append(')');

            spacer.subtract(OPERATOR_ATTRIBUTES_INDENT);

            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

    private static List<String> beautifyBitSet(ImmutableBitSet bitSet, RelDataType rowType) {
        return bitSet.stream()
                .mapToObj(rowType.getFieldNames()::get)
                .collect(Collectors.toList());
    }

    private static String beautifyAggCall(AggregateCall call, RexShuttle inputRefRewriter, RelDataType rowType) {
        StringBuilder buf = new StringBuilder(call.getAggregation().toString());
        buf.append('(');

        if (call.isApproximate()) {
            buf.append("APPROXIMATE ");
        }
        if (call.isDistinct()) {
            buf.append(call.getArgList().isEmpty() ? "DISTINCT" : "DISTINCT ");
        }
        int i = -1;
        for (RexNode rexNode : call.rexList) {
            if (++i > 0) {
                buf.append(", ");
            }
            buf.append(rexNode.accept(inputRefRewriter));
        }
        for (Integer arg : call.getArgList()) {
            if (++i > 0) {
                buf.append(", ");
            }
            buf.append(rowType.getFieldNames().get(arg));
        }
        buf.append(')');
        if (call.distinctKeys != null) {
            buf.append(" WITHIN DISTINCT (");
            for (Ord<Integer> key : Ord.zip(call.distinctKeys)) {
                buf.append(key.i > 0 ? ", " : "")
                        .append(rowType.getFieldNames().get(key.e));
            }
            buf.append(')');
        }
        if (call.hasCollation()) {
            buf.append(" WITHIN GROUP (").append(beautifyCollation(call.collation, rowType)).append(')');
        }
        if (call.hasFilter()) {
            buf.append(" FILTER ").append(rowType.getFieldNames().get(call.filterArg));
        }
        return buf.toString();
    }

    private static List<String> beautifyCollation(RelCollation collation, RelDataType rowType) {
        return collation.getFieldCollations().stream().map(fc -> {
            StringBuilder sb = new StringBuilder(rowType.getFieldNames().get(fc.getFieldIndex()))
                    .append(' ').append(fc.direction.shortString);

            if (fc.nullDirection != fc.direction.defaultNullDirection()) {
                sb.append(" NULLS ").append(fc.nullDirection);
            }

            return sb.toString();
        }).collect(Collectors.toList());
    }

    private static String beautifyDistribution(IgniteDistribution distribution, RelDataType rowType) {
        StringBuilder sb = new StringBuilder();
        sb.append(distribution.label());

        if (!distribution.getKeys().isEmpty()) {
            sb.append(" by [");

            boolean shouldAppendComma = false;
            for (int idx : distribution.getKeys()) {
                if (shouldAppendComma) {
                    sb.append(", ");
                }

                sb.append(rowType.getFieldNames().get(idx));

                shouldAppendComma = true;
            }

            sb.append(']');
        }

        return sb.toString();
    }

    private RelTreeToTextWriter() {
        throw new AssertionError("Should not be called");
    }
}
