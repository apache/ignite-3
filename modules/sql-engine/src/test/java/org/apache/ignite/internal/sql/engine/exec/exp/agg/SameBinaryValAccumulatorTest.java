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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.SameVal;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the variant of {@code SAME_VAL(SUBQUERY)} accumulator function that works for binary types.
 */
public class SameBinaryValAccumulatorTest {

    private static List<RelDataType> binaryTypes() {
        return List.of(
                Commons.typeFactory().createSqlType(SqlTypeName.BINARY),
                Commons.typeFactory().createSqlType(SqlTypeName.VARBINARY)
        );
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    public void test(RelDataType type) {
        StatefulAccumulator acc = newCall(type);

        acc.add(new Object[]{new byte[]{1, 2}});
        acc.add(new Object[]{new byte[]{1, 2}});

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.",
                () -> acc.add(new Object[]{new byte[]{1, 2, 3}}));

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.",
                () -> acc.add(new Object[]{null}));

        assertArrayEquals(new byte[]{1, 2}, (byte[]) acc.end());
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    public void nullFirst(RelDataType type) {
        StatefulAccumulator acc = newCall(type);

        acc.add(new Object[]{null});
        acc.add(new Object[]{null});

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.",
                () -> acc.add(new Object[]{new byte[]{1, 2, 3}}));

        assertNull(acc.end());
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    public void empty(RelDataType type) {
        StatefulAccumulator acc = newCall(type);

        assertNull(acc.end());
    }

    private StatefulAccumulator newCall(RelDataType type) {
        Supplier<Accumulator> supplier = SameVal.newAccumulator(type);
        return new StatefulAccumulator(supplier);
    }
}
