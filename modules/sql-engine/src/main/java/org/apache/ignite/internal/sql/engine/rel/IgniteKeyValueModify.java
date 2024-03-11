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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappingService;
import org.apache.ignite.internal.tx.InternalTransaction;

/**
 * Relational operator that represents simple modification that can be transacted
 * to Key-Value operation.
 *
 * <p>Note: at the moment, KV api requires an actual transaction which is object
 * of type {@link InternalTransaction} while distributed execution has access to
 * only certain {@link TxAttributes attributes} of a transaction. Given that node is
 * not supposed to be a part of distributed query plan, the following parts were
 * deliberately omitted:<ul>
 *     <li>this class doesn't implement {@link SourceAwareIgniteRel}, making it impossible
 *     to map properly by {@link MappingService}</li>
 *     <li>de-serialisation constructor is omitted (see {@link IgniteTableModify#IgniteTableModify(RelInput)}
 *     as example)</li>
 * </ul>
 */
public class IgniteKeyValueModify extends AbstractRelNode implements IgniteRel {
    private static final String REL_TYPE_NAME = "KeyValueModify";

    /** Enumeration of supported modification operations. */
    public enum Operation {
        PUT
    }

    private final RelOptTable table;
    private final Operation operation;
    private final List<RexNode> expressions;

    /**
     * Constructor.
     *
     * @param cluster A cluster this relation belongs to.
     * @param traits A set of traits this node satisfies.
     * @param table A target table.
     * @param operation Type of the modification operation.
     * @param expressions List of expressions representing either full row or only a key
     *      depending on particular operation.
     */
    public IgniteKeyValueModify(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table,
            Operation operation,
            List<RexNode> expressions
    ) {
        super(cluster, traits);

        this.table = table;
        this.operation = operation;
        this.expressions = expressions;
    }

    @Override public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType(
                SqlKind.INSERT, getCluster().getTypeFactory());
    }

    /** {@inheritDoc} */
    @Override
    public RelOptTable getTable() {
        return table;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        assert inputs.isEmpty() : inputs;

        return new IgniteKeyValueModify(cluster, getTraitSet(), table, operation, expressions);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("table", table.getQualifiedName())
                .item("operation", operation)
                .item("expressions", expressions);
    }

    /** {@inheritDoc} */
    @Override
    public String getRelTypeName() {
        return REL_TYPE_NAME;
    }

    /**
     * Returns a list of expressions representing either full row or only a key
     * depending on particular operation.
     *
     * @return List of expressions.
     */
    public List<RexNode> expressions() {
        return expressions;
    }
}
