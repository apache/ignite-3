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

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Function call validation.
 */
public class FunctionsTest extends AbstractPlannerTest {

    /** Opportunistic eliminations of casts are not performed. */
    @TestFactory
    public Stream<DynamicTest> testCastIsPreserved() {
        return Stream.of(
                expr("CAST(? AS VARCHAR(3))", "12345").project("CAST(?0):VARCHAR(3) CHARACTER SET \"UTF-8\"")
        );
    }

    /** Legal SUBSTR invocations. */
    @TestFactory
    public Stream<DynamicTest> testSubstrValid() {
        return Stream.of(
                expr("SUBSTR('123456789', 1)").ok(),
                expr("SUBSTR('123456789', 1, 2)").ok(),
                expr("SUBSTR('123456789', ?, 2)", 1).ok(),
                expr("SUBSTR('123456789', ?, ?)", 1, 2).ok(),
                expr("SUBSTR(?, ?, ?)", "123456789", 1, 2).ok(),

                expr("SUBSTR(NULL, 1, 2)").project("SUBSTR(null:NULL, 1, 2)"),
                expr("SUBSTR(?, 1)", new Object[]{null}).project("SUBSTR(?0, 1)"),
                expr("SUBSTR('123456789', ?)", new Object[]{null}).project("SUBSTR(_UTF-8'123456789', ?0)")
        );
    }

    /** Illegal SUBSTR invocations. */
    @TestFactory
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19469")
    public Stream<DynamicTest> testSubstrInvalid() {
        String incompatibleTypes = "Values passed to SUBSTR operator must have compatible types.";
        String canNotApplyToTypes = "Cannot apply 'SUBSTR' to arguments of type ";

        return Stream.of(
                expr("SUBSTR('abcd', 1.1)").fails(canNotApplyToTypes),
                expr("SUBSTR('abcd', ?)", new BigDecimal("3")).fails(canNotApplyToTypes + "'SUBSTR(<CHAR(4)>, <DECIMAL(32767, 0)>)"),
                expr("SUBSTR('abcd', ?)", new BigDecimal("3.0")).fails(canNotApplyToTypes + "'SUBSTR(<CHAR(4)>, <DECIMAL(32767, 0)>)"),
                expr("SUBSTR('abcd', ?)", 3.0f).fails(canNotApplyToTypes + "'SUBSTR(<CHAR(4)>, <REAL>)"),

                expr("SUBSTR(?, 1)", 100).fails(incompatibleTypes),
                expr("SUBSTR('12345678', ?)", "abcd").fails(incompatibleTypes),
                expr("SUBSTR('12345678', 1, ?)", "abcd").fails(incompatibleTypes)
        );
    }

    /** Additonal Illegal SUBSTR invocations. */
    @TestFactory
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19469")
    public Stream<DynamicTest> testSubstrInvalid2() {
        // Move to testSubstrInvalid after the issue is fixed.
        String canNotApplyToTypes = "Cannot apply 'SUBSTR' to arguments of type ";

        return Stream.of(
                expr("SUBSTR(100, 1)").fails(canNotApplyToTypes),
                expr("SUBSTR('123456789', '1')").fails(canNotApplyToTypes),
                expr("SUBSTR('123456789', 1, '2')").fails(canNotApplyToTypes),

                expr("SUBSTR('0000'::UUID, 1)").fails(canNotApplyToTypes),
                expr("SUBSTR('asdasdsa', ?, 1)", new UUID(0, 0)).fails(canNotApplyToTypes),
                expr("SUBSTR('asdasdsa', 1, ?)", new UUID(0, 0)).fails(canNotApplyToTypes)
        );
    }

    /**
     * A shorthand for {@code SELECT sql(expr, params)}.
     */
    private StatementChecker expr(String expr, Object... params) {
        return sql("SELECT " + expr, params);
    }
}
