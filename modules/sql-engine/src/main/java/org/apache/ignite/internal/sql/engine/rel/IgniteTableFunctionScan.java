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

package org.apache.ignite.internal.sql.engine.rel;

import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.exec.mapping.QuerySplitter;
import org.apache.ignite.internal.sql.engine.rel.explain.IgniteRelWriter;

/**
 * Relational operator for table function scan.
 */
public class IgniteTableFunctionScan extends TableFunctionScan implements SourceAwareIgniteRel {
    private static final String REL_TYPE_NAME = "TableFunctionScan";

    /** Default estimate row count. */
    private static final int ESTIMATE_ROW_COUNT = 100;

    private final long sourceId;

    /**
     * Creates a TableFunctionScan.
     */
    public IgniteTableFunctionScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            RexNode call,
            RelDataType rowType
    ) {
        this(-1L, cluster, traits, call, rowType);
    }

    /**
     * Creates a new Scan over function.
     *
     * @param sourceId An identifier of a source of rows. Will be assigned by {@link QuerySplitter}.
     * @param cluster Cluster that this relational expression belongs to.
     * @param traits A set of particular properties this relation satisfies.
     * @param call A call to a function emitting the rows to scan over.
     * @param rowType Row type for tuples produced by this rel.
     *
     * @see QuerySplitter
     */
    private IgniteTableFunctionScan(
            long sourceId,
            RelOptCluster cluster,
            RelTraitSet traits,
            RexNode call,
            RelDataType rowType
    ) {
        super(cluster, traits, List.of(), call, null, rowType, null);

        this.sourceId = sourceId;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableFunctionScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null) {
            sourceId = ((Number) srcIdObj).longValue();
        } else {
            sourceId = -1;
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableFunctionScan(cluster, getTraitSet(), getCall(), getRowType());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteTableFunctionScan(sourceId, getCluster(), getTraitSet(), getCall(), getRowType());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public TableFunctionScan copy(RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall,
            Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings) {
        assert nullOrEmpty(inputs);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("sourceId", sourceId, sourceId != -1);
    }

    /** {@inheritDoc} */
    @Override
    public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return ESTIMATE_ROW_COUNT;
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    @Override
    public IgniteRelWriter explain(IgniteRelWriter writer) {
        return writer.addInvocation(getCall());
    }
}
