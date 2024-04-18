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

package org.apache.ignite.internal.sql.engine.rule;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Rule that translates logical modify to the similar one but in Ignite's convention.
 */
public class TableModifyConverterRule extends AbstractIgniteConverterRule<LogicalTableModify> {
    public static final RelOptRule INSTANCE = new TableModifyConverterRule();

    /**
     * Creates a ConverterRule.
     */
    public TableModifyConverterRule() {
        super(LogicalTableModify.class, "TableModifyConverterRule");
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalTableModify rel) {
        RelOptCluster cluster = rel.getCluster();
        RelOptTable relTable = rel.getTable();
        IgniteTable igniteTable = relTable.unwrap(IgniteTable.class);
        assert igniteTable != null;

        IgniteDistribution distribution = igniteTable.distribution();

        if (rel.getOperation() == Operation.DELETE) {
            // To perform the delete, we need a row with key fields only.
            // Input distribution contains the indexes of the key columns according to the schema (i.e. for the full row).
            // Here we adjusting distribution keys so that a row containing only the key fields can be read.
            ImmutableIntList keyColumns = igniteTable.keyColumns();

            distribution = distribution.apply(Commons.projectedMapping(igniteTable.descriptor().columnsCount(), keyColumns));
        }

        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(distribution)
                .replace(RelCollations.EMPTY);

        RelNode input = convert(rel.getInput(), traits);

        IgniteTableModify tableModify = new IgniteTableModify(cluster, traits, relTable, input,
                rel.getOperation(), rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened());

        // Return IgniteTableModify w/o aggregation for tables with single distribution.
        if (igniteTable.distribution().equals(IgniteDistributions.single())) {
            return tableModify;
        } else {
            return createAggregate(tableModify, cluster);
        }
    }

    private static PhysicalNode createAggregate(IgniteTableModify tableModify, RelOptCluster cluster) {
        // We must aggregate the total number of modified rows returned by of instances of DML operations.

        RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);

        // 1) add a SUM0 aggregate on top of TableModify to compute the total number of modified rows.
        RelDataType rowType = tableModify.getRowType();
        RelDataTypeField modifiedRowsField = rowType.getFieldList().get(0);
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType rowCountType = modifiedRowsField.getType();
        RelDataType sumType = IgniteTypeSystem.INSTANCE.deriveSumType(typeFactory, rowCountType);

        AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM0, false, false,
                false, ImmutableList.of(0), -1, null, RelCollations.EMPTY, 0, tableModify,
                sumType, null);

        IgniteColocatedHashAggregate sumAgg = new IgniteColocatedHashAggregate(
                cluster,
                outTrait.replace(IgniteDistributions.single()),
                convert(tableModify, inTrait.replace(IgniteDistributions.single())),
                ImmutableBitSet.of(),
                List.of(ImmutableBitSet.of()),
                List.of(sum)
        );

        // 2) add a projection on top of the SUM0 aggregate that converts the result of SUM aggregate back to BIGINT
        // (because the modified number of rows returned by DML operations is BIGINT)
        RelDataType typeOfSum = modifiedRowsField.getType();
        RelDataType convertedRowType = typeFactory.createStructType(List.of(Map.entry(modifiedRowsField.getName(), typeOfSum)));

        RexBuilder rexBuilder = Commons.rexBuilder();
        RexInputRef sumRef = rexBuilder.makeInputRef(sumAgg, 0);
        RexNode rexNode = rexBuilder.makeCast(typeOfSum, sumRef);
        List<RexNode> projections = Collections.singletonList(rexNode);

        return new IgniteProject(cluster, outTrait.replace(IgniteDistributions.single()), sumAgg, projections, convertedRowType);
    }
}
