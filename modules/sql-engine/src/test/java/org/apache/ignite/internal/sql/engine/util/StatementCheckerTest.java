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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.StatementChecker.SqlPrepare;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

/**
 * Tests for {@link StatementChecker}.
 */
public class StatementCheckerTest {

    private final RelNode dummyNode = Mockito.mock(IgniteRel.class);

    private final SqlPrepare sqlPrepare = Mockito.mock(SqlPrepare.class);

    /** Validation check should pass. */
    @Test
    public void testOk() throws Throwable {
        DynamicTest test = newChecker().sql("SELECT 1").ok();
        assertEquals("OK SELECT 1", test.getDisplayName(), "display name");

        when(sqlPrepare.prepare(any(IgniteSchema.class), any(String.class), any(List.class))).thenReturn(dummyNode);

        test.getExecutable().execute();
    }

    /** Validation check should pass - error any error is accepted. */
    @Test
    public void testFailAnyError() throws Throwable {
        DynamicTest test = newChecker().sql("SELECT").fails();
        assertEquals("ERR SELECT", test.getDisplayName(), "display name");

        when(sqlPrepare.prepare(any(IgniteSchema.class), any(String.class), any(List.class)))
                .thenThrow(new RuntimeException());

        test.getExecutable().execute();
    }

    /** Validation fails - error mismatch. */
    @Test
    public void testFailErrorMismatch() throws Throwable {
        DynamicTest test = newChecker()
                .sql("SELECT")
                .fails(Matchers.nullValue());

        assertEquals("ERR SELECT", test.getDisplayName(), "display name");

        RuntimeException cause = new RuntimeException("Invalid statement");
        when(sqlPrepare.prepare(any(IgniteSchema.class), any(String.class), any(List.class)))
                .thenThrow(cause);

        AssertionFailedError t = assertThrows(AssertionFailedError.class, () -> test.getExecutable().execute());
        expectTestLocationIsPresent(t);
    }

    /** Validation success check fails - plan does not match. */
    @Test
    public void testOkCheckThrows() throws Exception {
        DynamicTest test = newChecker().sql("SELECT 1").ok(((node) -> {
            throw new AssertionFailedError("Error");
        }));
        assertEquals("OK SELECT 1", test.getDisplayName(), "display name");

        when(sqlPrepare.prepare(any(IgniteSchema.class), any(String.class), any(List.class)))
                .thenReturn(dummyNode);

        AssertionFailedError t = assertThrows(AssertionFailedError.class, () -> test.getExecutable().execute());
        expectTestLocationIsPresent(t);
    }

    /** Validation success check fails - prepare throws an error. */
    @Test
    public void testOkPrepareThrows() throws Exception {
        DynamicTest test = newChecker().sql("SELECT 1").ok();

        assertEquals("OK SELECT 1", test.getDisplayName(), "display name");

        when(sqlPrepare.prepare(any(IgniteSchema.class), any(String.class), any(List.class)))
                .thenThrow(new RuntimeException("Invalid statement"));

        RuntimeException t = assertThrows(RuntimeException.class, () -> test.getExecutable().execute());
        expectTestLocationIsPresent(t);
    }

    private static void expectTestLocationIsPresent(Throwable t) {
        Throwable[] suppressed = t.getSuppressed();

        if (suppressed.length < 1) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);

            Assertions.fail("Test location is missing:\n" + sw);
        }

        assertEquals("Statement check failed", suppressed[0].getMessage(), "Test location");
    }

    private StatementChecker newChecker() {
        return new StatementChecker(sqlPrepare);
    }
}
