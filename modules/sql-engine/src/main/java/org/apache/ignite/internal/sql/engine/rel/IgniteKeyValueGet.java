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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;

/**
 * Relational operator that represents lookup by a primary key.
 *
 * <p>Note: at the moment, KV api requires an actual transaction which is object
 * of type {@link InternalTransaction} while distributed execution has access to
 * only certain {@link TxAttributes attributes} of a transaction. Given that node is
 * not supposed to be a part of distributed query plan, the following parts were
 * deliberately omitted:<ul>
 *     <li>this class doesn't implement {@link SourceAwareIgniteRel}, making it impossible
 *     to map properly by {@link MappingService}</li>
 *     <li>de-serialisation constructor is omitted (see {@link ProjectableFilterableTableScan#ProjectableFilterableTableScan(RelInput)}
 *     as example)</li>
 * </ul>
 */
public class IgniteKeyValueGet extends ProjectableFilterableTableScan implements IgniteRel {
    private static final String REL_TYPE_NAME = "KeyValueGet";

    private final List<RexNode> keyExpressions;

    /**
     * Constructor.
     *
     * @param cluster A cluster this relation belongs to.
     * @param traits A set of traits this node satisfies.
     * @param table A source table.
     * @param hints List of hints related to a relational node.
     * @param keyExpressions List of expressions representing primary key.
     * @param proj Optional projection to transform the row.
     * @param cond Optional condition to do post-filtration.
     * @param requiredColumns Optional set required fields to do trimming unused columns.
     */
    public IgniteKeyValueGet(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            List<RelHint> hints,
            List<RexNode> keyExpressions,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        super(cluster, traits, hints, table, proj, cond, requiredColumns);

        this.keyExpressions = keyExpressions;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteKeyValueGet(cluster, getTraitSet(), getTable(), getHints(), keyExpressions, projects, condition, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteKeyValueGet withHints(List<RelHint> hintList) {
        return new IgniteKeyValueGet(
                getCluster(), getTraitSet(), getTable(), hintList, keyExpressions, projects, condition, requiredColumns
        );
    }

    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        return super.explainTerms0(pw)
                .item("key", keyExpressions);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    /**
     * Returns a list of expressions in the order of a primary key columns of related table
     * to use as lookup key.
     *
     * @return List of expressions representing lookup key.
     */
    public List<RexNode> keyExpressions() {
        return keyExpressions;
    }
}
