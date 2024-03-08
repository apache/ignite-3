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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.ignite.internal.sql.engine.exec.mapping.QuerySplitter;

/**
 * IgniteValues.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class IgniteValues extends Values implements SourceAwareIgniteRel {
    private static final String REL_TYPE_NAME = "Values";

    private final long sourceId;

    /**
     * Creates a new Values.
     *
     * <p>Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this call, otherwise bad things will happen.
     *
     * @param cluster Cluster that this relational expression belongs to.
     * @param rowType Row type for tuples produced by this rel.
     * @param tuples  2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is
     *               one tuple; all tuples must be of same length, conforming to rowType.
     */
    public IgniteValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traits) {
        this(-1L, cluster, rowType, tuples, traits);
    }

    /**
     * Creates a new Values.
     *
     * <p>Note that tuples passed in become owned by this rel (without a deep copy),
     * so caller must not modify them after this call, otherwise bad things will happen.
     *
     * @param sourceId An identifier of a source of rows. Will be assigned by {@link QuerySplitter}.
     * @param cluster Cluster that this relational expression belongs to.
     * @param rowType Row type for tuples produced by this rel.
     * @param tuples  2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is
     *               one tuple; all tuples must be of same length, conforming to rowType.
     * @param traits A set of particular properties this relation satisfies.
     *
     * @see QuerySplitter
     */
    private IgniteValues(long sourceId, RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);

        this.sourceId = sourceId;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteValues(RelInput input) {
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
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteValues(cluster, getRowType(), getTuples(), getTraitSet());
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteValues(sourceId, getCluster(), getRowType(), getTuples(), getTraitSet());
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
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }
}
