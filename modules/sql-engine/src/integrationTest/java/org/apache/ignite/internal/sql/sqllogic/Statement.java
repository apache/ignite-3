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

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;

/**
 * Statement command executes an SQL statement/an SQL query and expects either an error or a successful result.
 * <pre>
 *     # an SQL statement/query must returns successful result
 *     statement ok
 *     SELECT 1
 * </pre>
 * <pre>
 *     # an SQL statement/query must returns and error
 *     statement error
 *     SELECT col
 * </pre>
 * <pre>
 *     # Extension.
 *     # an SQL statement/query must returns and error that contains the specified substring.
 *     statement error: Column 'COL' not found in any table
 *     SELECT col
 * </pre>
 */
final class Statement extends Command {
    private final List<String> queries;

    private final ExpectedStatementStatus expected;

    Statement(Script script, ScriptContext ctx, String[] cmd) throws IOException {
        super(script.scriptPosition());

        String expectedStatus = cmd[1];
        switch (expectedStatus) {
            case "ok":
                expected = ExpectedStatementStatus.ok();
                break;
            case "error":
                expected = ExpectedStatementStatus.error();
                break;
            default:
                if (expectedStatus.startsWith("error:")) {
                    String[] errorMessage = Arrays.copyOfRange(cmd, 2, cmd.length);
                    expected = ExpectedStatementStatus.error(String.join(" ", errorMessage).trim());
                } else {
                    throw script.reportInvalidCommand("Statement argument should be 'ok' or 'error'", cmd);
                }
        }

        queries = new ArrayList<>();

        while (script.ready()) {
            String s = script.nextLine();

            if (Strings.isNullOrEmpty(s)) {
                break;
            }

            queries.add(s);
        }
    }

    @Override
    void execute(ScriptContext ctx) {
        for (String qry : queries) {
            String[] toks = qry.split("\\s+");

            if ("PRAGMA".equals(toks[0])) {
                String[] pragmaParams = toks[1].split("=");

                if ("null".equals(pragmaParams[0])) {
                    ctx.nullLbl = pragmaParams[1];
                } else {
                    ctx.log.info("Ignore: " + Arrays.toString(pragmaParams));
                }

                continue;
            }

            if (expected.successful) {
                try {
                    ctx.executeQuery(qry);
                } catch (Throwable e) {
                    Assertions.fail("Not expected result at: " + posDesc + ". Statement: " + qry, e);
                }
            } else {
                Throwable err = Assertions.assertThrows(
                        Throwable.class,
                        () -> ctx.executeQuery(qry),
                        "Not expected result at: " + posDesc + ". Statement: " + qry + ". No error occurred");

                assertThat(
                        "Not expected result at: " + posDesc + ". Statement: " + qry + ". Expected: " + expected.errorMessage,
                        err.getMessage(), expected.errorMessage);
            }
        }
    }

    @Override
    public String toString() {
        return "Statement ["
                + "queries=" + queries
                + ", expected=" + expected
                + ']';
    }

    private static class ExpectedStatementStatus {

        private final boolean successful;

        private final org.hamcrest.Matcher<String> errorMessage;

        ExpectedStatementStatus(boolean successful, @Nullable org.hamcrest.Matcher<String> errorMessage) {
            if (successful && errorMessage != null) {
                throw new IllegalArgumentException("Successful status with error message: " + errorMessage);
            }
            this.successful = successful;
            this.errorMessage = errorMessage;
        }

        static ExpectedStatementStatus ok() {
            return new ExpectedStatementStatus(true, null);
        }

        static ExpectedStatementStatus error() {
            return new ExpectedStatementStatus(false, anyOf(nullValue(String.class), any(String.class)));
        }

        static ExpectedStatementStatus error(String errorMessage) {
            return new ExpectedStatementStatus(false, containsString(errorMessage));
        }

        @Override
        public String toString() {
            if (successful) {
                return "ok";
            } else {
                return "error:" + errorMessage;
            }
        }
    }
}
