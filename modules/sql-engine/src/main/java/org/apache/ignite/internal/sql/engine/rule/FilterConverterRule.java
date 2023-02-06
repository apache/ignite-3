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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.sql.engine.rel.IgniteConvention;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;

/**
 * FilterConverterRule.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FilterConverterRule extends AbstractIgniteConverterRule<LogicalFilter> {
    public static final RelOptRule INSTANCE = new FilterConverterRule();

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public FilterConverterRule() {
        super(LogicalFilter.class, "FilterConverterRule");
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalFilter rel) {
        RelOptCluster cluster = rel.getCluster();

        RelTraitSet traits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single());

        return new IgniteFilter(
                cluster,
                traits,
                convert(rel.getInput(), traits),
                rel.getCondition()
        );
    }
}
