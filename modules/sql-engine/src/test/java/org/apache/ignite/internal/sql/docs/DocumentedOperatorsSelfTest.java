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

package org.apache.ignite.internal.sql.docs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.ignite.internal.sql.docs.DocumentedOperators.DocumentedOperator;
import org.apache.ignite.internal.sql.docs.DocumentedOperators.Signatures;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

/** Tests for {@link DocumentedOperators}. */
public class DocumentedOperatorsSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void testFormatting() {
        assertEquals("<cmp> > <cmp>", DocumentedOperators.formatTypeNames("<Cmp> > <CmP>"));
        assertEquals("!<cmp>", DocumentedOperators.formatTypeNames("!<CmP>"));
        assertEquals("- <cmp>", DocumentedOperators.formatTypeNames("- <CmP>"));
        assertEquals("Func(<cmp>)", DocumentedOperators.formatTypeNames("Func(<CMP>)"));
        assertEquals("Func(<abc>, <def>)", DocumentedOperators.formatTypeNames("Func(<Abc>, <dEf>)"));

        assertEquals(
                List.of("<comparable_type> < <comparable_type>"),
                Signatures.makeSignatures(new DocumentedOperator(SqlStdOperatorTable.LESS_THAN, "<", true)).sigs
        );
        assertEquals(
                List.of("<comparable_type> > <comparable_type>"),
                Signatures.makeSignatures(new DocumentedOperator(SqlStdOperatorTable.GREATER_THAN, ">", true)).sigs
        );
        assertEquals(
                List.of("MIN(<comparable_type>)"),
                Signatures.makeSignatures(new DocumentedOperator(SqlStdOperatorTable.MIN, "MIN", true)).sigs
        );
    }

    @Test
    public void testValidationFunctionOk() {
        SqlOperator add = SqlStdOperatorTable.PLUS;
        SqlOperator union = SqlStdOperatorTable.UNION;
        SqlOperator sub = SqlStdOperatorTable.MINUS;

        SqlOperatorTable table = SqlOperatorTables.of(add, sub, union);

        DocumentedOperators ops = new DocumentedOperators("Test");
        ops.add(add);
        ops.internal(union);
        ops.add(sub);

        DocumentedOperators.validateOperatorList(table, List.of(ops), "some file");
    }

    @Test
    public void testValidationFunctionFailsWhenSomeOpsAreNotIncluded() {
        SqlOperator add = SqlStdOperatorTable.PLUS;
        SqlOperator mul = SqlStdOperatorTable.MULTIPLY;
        SqlOperator sub = SqlStdOperatorTable.MINUS;

        SqlOperatorTable table = SqlOperatorTables.of(add, sub, mul);

        DocumentedOperators ops = new DocumentedOperators("Test");
        ops.add(add);
        ops.add(mul);

        // validateOperatorList should fail because minus operator is missing from DocumentedOperators.
        try {
            DocumentedOperators.validateOperatorList(table, List.of(ops), "some file");
        } catch (AssertionFailedError err) {

            assertThat("Error:\n" + err.getMessage(), err.getMessage(), Matchers.allOf(
                    containsString("- class: " + sub.getClass().getCanonicalName()),
                    not(containsString("+ class: " + add.getClass().getCanonicalName())),
                    not(containsString("* class: " + mul.getClass().getCanonicalName()))
            ));
            return;
        }

        fail();
    }
}
