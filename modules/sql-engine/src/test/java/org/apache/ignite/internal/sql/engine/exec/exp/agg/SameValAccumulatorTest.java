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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.function.Supplier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators.SameVal;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code SAME_VAL(SUBQUERY)} accumulator function.
 */
public class SameValAccumulatorTest {

    @Test
    public void test() {
        StatefulAccumulator acc = newCall();
        acc.add("1");
        acc.add("1");

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.", () -> acc.add("2"));
        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.", () -> acc.add(new Object[]{null}));

        assertEquals("1", acc.end());
    }

    @Test
    public void nullFirst() {
        StatefulAccumulator acc = newCall();

        acc.add(new Object[]{null});
        acc.add(new Object[]{null});

        assertThrowsSqlException(Sql.RUNTIME_ERR, "Subquery produced different values.", () -> acc.add("2"));

        assertNull(acc.end());
    }

    @Test
    public void empty() {
        StatefulAccumulator acc = newCall();

        assertNull(acc.end());
    }

    private StatefulAccumulator newCall() {
        Supplier<Accumulator> supplier = SameVal.newAccumulator(Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR));
        return new StatefulAccumulator(supplier);
    }
}
