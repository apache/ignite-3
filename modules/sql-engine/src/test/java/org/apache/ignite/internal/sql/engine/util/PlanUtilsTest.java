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

package org.apache.ignite.internal.sql.engine.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PlanUtils}.
 */
public class PlanUtilsTest {

    private final IgniteTypeFactory typeFactory = Commons.typeFactory();

    private final Accumulators accumulators = new Accumulators(typeFactory);

    @Test
    public void testHashAggRowType() {
        RelDataType inputType = new RelDataTypeFactory.Builder(typeFactory)
                .add("f1", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("f2", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("f3", typeFactory.createSqlType(SqlTypeName.TINYINT))
                .add("f4", typeFactory.createSqlType(SqlTypeName.VARBINARY))
                .build();

        AggregateCall call1 = newCall(typeFactory.createSqlType(SqlTypeName.BIGINT));
        Accumulator acc1 = accumulators.accumulatorFactory(call1).get();

        RelDataType expectedType = new RelDataTypeFactory.Builder(typeFactory)
                .add("f1", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("f2", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("f4", typeFactory.createSqlType(SqlTypeName.VARBINARY))
                .add("_ACC0", acc1.returnType(typeFactory))
                .add("_GROUP_ID", typeFactory.createSqlType(SqlTypeName.TINYINT))
                .build();

        ImmutableBitSet group1 = ImmutableBitSet.of(0, 1);
        ImmutableBitSet group2 = ImmutableBitSet.of(1);
        ImmutableBitSet group3 = ImmutableBitSet.of(3);

        RelDataType rowType = PlanUtils.createHashAggRowType(List.of(group1, group2, group3), typeFactory,
                inputType, Arrays.asList(call1));

        assertEquals(expectedType, rowType);
    }

    @Test
    public void testSortAggRowType() {
        RelDataType inputType = new RelDataTypeFactory.Builder(typeFactory)
                .add("f1", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("f2", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("f3", typeFactory.createSqlType(SqlTypeName.TINYINT))
                .add("f4", typeFactory.createSqlType(SqlTypeName.VARBINARY))
                .build();

        AggregateCall call1 = newCall(typeFactory.createSqlType(SqlTypeName.BIGINT));
        Accumulator acc1 = accumulators.accumulatorFactory(call1).get();

        RelDataType expectedType = new RelDataTypeFactory.Builder(typeFactory)
                .add("f1", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("f2", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("f4", typeFactory.createSqlType(SqlTypeName.VARBINARY))
                .add("_ACC0", acc1.returnType(typeFactory))
                .build();

        ImmutableBitSet group = ImmutableBitSet.of(0, 1, 3);

        RelDataType rowType = PlanUtils.createSortAggRowType(group, typeFactory, inputType, Arrays.asList(call1));
        assertEquals(expectedType, rowType);
    }

    private static AggregateCall newCall(RelDataType type) {
        return AggregateCall.create(
                SqlStdOperatorTable.SUM,
                false,
                false,
                false,
                ImmutableList.of(),
                List.of(0),
                -1,
                ImmutableBitSet.of(),
                RelCollations.EMPTY,
                type,
                null);
    }
}
