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

package org.apache.ignite.internal.sql.engine.prepare;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.table.QualifiedNameHelper;
import org.jetbrains.annotations.Nullable;

/** Printer that dumps relation tree to a text representation for EXPLAIN command output. */
public class ExplainRelAsTextWriter extends RelWriterImpl {
    private static final int NEXT_OPERATOR_INDENT = 2;
    private static final int OPERATOR_ATTRIBUTES_INDENT = 2 * NEXT_OPERATOR_INDENT;

    ExplainRelAsTextWriter(PrintWriter pw) {
        super(pw, SqlExplainLevel.ALL_ATTRIBUTES, false);
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
        List<RelNode> inputs = rel.getInputs();
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        if (!mq.isVisibleInExplain(rel, detailLevel)) {
            // render children in place of this, at same level
            explainInputs(inputs);
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        s.append(rel.getRelTypeName());

        spacer.add(OPERATOR_ATTRIBUTES_INDENT);

        for (Pair<String, @Nullable Object> value : values) {
            if (value.right instanceof RelNode) {
                continue;
            }

            s.append(System.lineSeparator());
            spacer.spaces(s);

            s.append(value.left).append(": ").append(beautify(rel, value.right));
        }

        s.append(System.lineSeparator());
        spacer.spaces(s);

        s.append("est: (rows=").append(BigDecimal.valueOf(mq.getRowCount(rel)).setScale(0, RoundingMode.HALF_UP)).append(')');

        spacer.subtract(OPERATOR_ATTRIBUTES_INDENT);

        pw.println(s);

        spacer.add(NEXT_OPERATOR_INDENT);
        explainInputs(inputs);
        spacer.subtract(NEXT_OPERATOR_INDENT);
    }

    private static @Nullable Object beautify(RelNode node, @Nullable Object object) {
        if (object == null) {
            return null;
        }

        if (object instanceof RelOptTable) {
            List<String> parts = ((RelOptTable) object).getQualifiedName();

            assert parts.size() == 2 : parts;

            return QualifiedNameHelper.fromNormalized(parts.get(0), parts.get(1)).toCanonicalForm();
        }

        RelDataType rowTypeToResolveExpressions;
        RelDataType rowTypeToResolveBitSet;
        if (node instanceof ProjectableFilterableTableScan) {
            IgniteDataSource dataSource = ((ProjectableFilterableTableScan) node).getTable().unwrap(IgniteDataSource.class);

            assert dataSource != null;

            rowTypeToResolveBitSet = dataSource.getRowType(Commons.typeFactory(node.getCluster()));
            rowTypeToResolveExpressions = dataSource.getRowType(Commons.typeFactory(node.getCluster()),
                    ((ProjectableFilterableTableScan) node).requiredColumns());
        } else if (node instanceof Join) {
            rowTypeToResolveExpressions = node.getRowType();
            rowTypeToResolveBitSet = rowTypeToResolveExpressions;
        } else if (node.getInputs().size() == 1) {
            rowTypeToResolveExpressions = node.getInputs().get(0).getRowType();
            rowTypeToResolveBitSet = rowTypeToResolveExpressions;
        } else {
            rowTypeToResolveExpressions = null;
            rowTypeToResolveBitSet = null;
        }

        if (rowTypeToResolveExpressions != null) {
            RexShuttle shuttle = new RexShuttle() {
                @Override
                public RexNode visitLocalRef(RexLocalRef ref) {
                    return new NamedRexSlot(rowTypeToResolveExpressions.getFieldNames().get(ref.getIndex()), ref.getIndex(), ref.getType());
                }

                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    return new NamedRexSlot(rowTypeToResolveExpressions.getFieldNames().get(ref.getIndex()), ref.getIndex(), ref.getType());
                }
            };

            if (object instanceof RexNode) {
                return ((RexNode) object).accept(shuttle);
            }

            if (object instanceof Collection) {
                Function<Object, Object> f = in -> {
                    if (in instanceof RexNode) {
                        return ((RexNode) in).accept(shuttle);
                    }

                    if (in instanceof AggregateCall) {
                        return beautifyAggCall((AggregateCall) in, shuttle, rowTypeToResolveExpressions);
                    }

                    return in;
                };

                return Commons.transform(new ArrayList<>((Collection<Object>) object), f);
            }

            if (object instanceof RelCollation) {
                return beautifyCollation((RelCollation) object, rowTypeToResolveExpressions);
            }

            if (object instanceof AggregateCall) {
                return beautifyAggCall((AggregateCall) object, shuttle, rowTypeToResolveExpressions);
            }
        }

        if (rowTypeToResolveBitSet != null) {
            if (object instanceof BitSet) {
                object = ImmutableBitSet.fromBitSet((BitSet) object);
            }

            if (object instanceof ImmutableBitSet) {
                object = ((ImmutableBitSet) object).toList().stream().map(rowTypeToResolveBitSet.getFieldNames()::get)
                        .collect(Collectors.toList());
            }
        }

        return object;
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

    private static String beautifyAggCall(AggregateCall call, RexShuttle inputRefRewriter, RelDataType rowType) {
        StringBuilder buf = new StringBuilder();
        if (!StringUtils.nullOrBlank(call.name)) {
            buf.append(call.name);
            buf.append('=');
        }

        buf.append(call.getAggregation().toString()).append('(');

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
                buf.append(key.i > 0 ? ", " : "");
                buf.append(rowType.getFieldNames().get(key.e));
            }
            buf.append(')');
        }
        if (call.hasCollation()) {
            buf.append(" WITHIN GROUP (");
            buf.append(beautifyCollation(call.collation, rowType));
            buf.append(')');
        }
        if (call.hasFilter()) {
            buf.append(" FILTER ");
            buf.append(rowType.getFieldNames().get(call.filterArg));
        }
        return buf.toString();
    }

    @SuppressWarnings("MethodOverridesInaccessibleMethodOfSuper")
    private void explainInputs(List<RelNode> inputs) {
        for (RelNode input : inputs) {
            input.explain(this);
        }
    }

    @Override
    public boolean nest() {
        return true;
    }

    private static class NamedRexSlot extends RexSlot {
        NamedRexSlot(String name, int index, RelDataType type) {
            super(name, index, type);
        }

        @Override
        public <R> R accept(RexVisitor<R> visitor) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public boolean equals(@Nullable Object obj) {
            return this == obj
                    || obj instanceof NamedRexSlot
                    && index == ((NamedRexSlot) obj).index;
        }

        @Override
        public int hashCode() {
            return index;
        }
    }
}
