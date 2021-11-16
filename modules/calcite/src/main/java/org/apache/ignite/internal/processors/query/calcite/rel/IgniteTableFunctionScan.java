/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rel;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Relational operator for table function scan.
 */
public class IgniteTableFunctionScan extends TableFunctionScan implements InternalIgniteRel {
    /** Default estimate row count. */
    private static final int ESTIMATE_ROW_COUNT = 100;
    
    /**
     * Creates a TableFunctionScan.
     */
    public IgniteTableFunctionScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            RexNode call,
            RelDataType rowType
    ) {
        super(cluster, traits, List.of(), call, null, rowType, null);
    }
    
    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableFunctionScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableFunctionScan(cluster, getTraitSet(), getCall(), getRowType());
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
    public double estimateRowCount(RelMetadataQuery mq) {
        return ESTIMATE_ROW_COUNT;
    }
}
