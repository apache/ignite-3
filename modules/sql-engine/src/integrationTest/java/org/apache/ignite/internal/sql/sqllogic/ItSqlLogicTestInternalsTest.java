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

package org.apache.ignite.internal.sql.sqllogic;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.sqllogic.ItSqlLogicTest.TestRunnerRuntime;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.Test;

/** Sql logic internals. */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlLogicTestInternalsTest extends BaseIgniteAbstractTest {
    /** Check if expected and returned results are mismatched, correct exception with list of returned results is raised. */
    @Test
    public void returnResults() throws Exception {
        TestRunnerRuntime runnerRuntime = mock(TestRunnerRuntime.class);
        IgniteSql sql = mock(IgniteSql.class);
        when(runnerRuntime.sql()).thenReturn(sql);
        ScriptContext ctx = new ScriptContext(runnerRuntime);

        File file = generateTempFile();

        try {
            IgniteStringBuilder sb = new IgniteStringBuilder();
            sb.app("query II").nl();
            sb.app("SELECT c1, c2 FROM (VALUES(1, 2), (2, 3)) t(c1, c2) ORDER BY c1").nl();
            sb.app("----").nl();
            sb.app("2\t3").nl();
            sb.app("1\t2").nl();

            Files.writeString(file.toPath(), sb.toString());

            try (Script script = new Script(file.toPath(), ctx)) {
                Query qry = new Query(script, ctx, new String[]{"query", "II"});
                List<List<?>> res = List.of(List.of(1, 2), List.of(2, 3));

                assertThrowsWithCause(
                        () -> qry.checkResultTuples(ctx, res),
                        AssertionError.class,
                        "Invalid results"
                );

                List<List<?>> resMore = List.of(List.of(1, 1), List.of(1, 2), List.of(2, 3));

                assertThrowsWithCause(
                        () -> qry.checkResultTuples(ctx, resMore),
                        AssertionError.class,
                        "Invalid results rows count"
                );
            }
        } finally {
            Files.delete(file.toPath());
        }
    }

    private static File generateTempFile() throws IOException {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String fileName = "test_sqllogic_test.test";
        File f = new File(tmpDir, fileName);
        if (f.exists()) {
            assert !f.isDirectory();
            Files.delete(f.toPath());
        }
        return f;
    }
}
