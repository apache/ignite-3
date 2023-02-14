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

package org.apache.ignite.internal.sql.engine.planner;

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;

/**
 * Base class for further planner test implementations.
 */
public abstract class AbstractAggregatePlannerTest extends AbstractPlannerTest {
    /**
     * Creates table with broadcast distribution.
     *
     * @param tblName Table name.
     * @return Table instance with broadcast distribution and multiple predefined columns.
     */
    protected TestTable createBroadcastTable(String tblName) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return createTable(tblName,
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("VAL0", f.createJavaType(Integer.class))
                        .add("VAL1", f.createJavaType(Integer.class))
                        .add("GRP0", f.createJavaType(Integer.class))
                        .add("GRP1", f.createJavaType(Integer.class))
                        .build(),
                DEFAULT_TBL_SIZE,
                IgniteDistributions.broadcast()
        );
    }

    /**
     * Creates table with specified affinity distribution.
     *
     * @param tblName Table name.
     * @return Table instance with specified affinity distribution and multiple predefined columns.
     */
    protected AbstractPlannerTest.TestTable createAffinityTable(String tblName) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        return createTable(tblName,
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("VAL0", f.createJavaType(Integer.class))
                        .add("VAL1", f.createJavaType(Integer.class))
                        .add("GRP0", f.createJavaType(Integer.class))
                        .add("GRP1", f.createJavaType(Integer.class))
                        .build(),
                DEFAULT_TBL_SIZE,
                IgniteDistributions.affinity(0, UUID.randomUUID(), DEFAULT_ZONE_ID)
        );
    }

    protected static Supplier<String> invalidPlanErrorMessage(IgniteRel phys) {
        return () -> "Invalid plan\n" + RelOptUtil.toString(phys, SqlExplainLevel.ALL_ATTRIBUTES);
    }

    enum AggregateAlgorithm {
        SORT(
                IgniteColocatedSortAggregate.class,
                IgniteMapSortAggregate.class,
                IgniteReduceSortAggregate.class,
                "MapReduceHashAggregateConverterRule",
                "ColocatedHashAggregateConverterRule"
        ),

        HASH(
                IgniteColocatedHashAggregate.class,
                IgniteMapHashAggregate.class,
                IgniteReduceHashAggregate.class,
                "MapReduceSortAggregateConverterRule",
                "ColocatedSortAggregateConverterRule"
        );

        public final Class<? extends IgniteColocatedAggregateBase> colocated;

        public final Class<? extends IgniteMapAggregateBase> map;

        public final Class<? extends IgniteReduceAggregateBase> reduce;

        public final String[] rulesToDisable;

        AggregateAlgorithm(
                Class<? extends IgniteColocatedAggregateBase> colocated,
                Class<? extends IgniteMapAggregateBase> map,
                Class<? extends IgniteReduceAggregateBase> reduce,
                String... rulesToDisable) {
            this.colocated = colocated;
            this.map = map;
            this.reduce = reduce;
            this.rulesToDisable = rulesToDisable;
        }
    }
}
