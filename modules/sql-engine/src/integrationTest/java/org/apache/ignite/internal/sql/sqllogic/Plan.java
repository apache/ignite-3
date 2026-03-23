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

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringBuilder;

/**
 * Derives a plan for given SQL query and expects it to match given plan.
 */
final class Plan extends Command {
    /** An SQL query string. **/
    private final String sql;

    private final String expectedPlan;

    private final boolean mapping;

    Plan(Script script, ScriptContext ctx, String[] cmd) throws IOException {
        super(script.scriptPosition());

        var sqlString = new IgniteStringBuilder();

        assert cmd.length == 2 : "unexpected line: " + cmd;

        String explainType = cmd[1];
        switch (explainType) {
            case "mapping":
                mapping = true;
                break;
            case "plan":
                mapping = false;
                break;
            default:
                throw script.reportInvalidCommand("Unsupported explain type", cmd);
        }

        // Read SQL query
        while (script.ready()) {
            String line = script.nextLine();

            // Check for typos. Otherwise we are going to get very confusing errors down the road.
            if (line.startsWith("--")) {
                if ("----".equals(line)) {
                    break;
                } else {
                    throw script.reportError("Invalid query terminator sequence", line);
                }
            }

            if (sqlString.length() > 0) {
                sqlString.nl();
            }

            sqlString.app(line);
        }

        this.sql = sqlString.toString();

        IgniteStringBuilder expectedResultBuilder = new IgniteStringBuilder();

        // Read expected results
        do {
            String s = script.nextLineWithoutTrim();

            if (mapping) {
                if ("----".equals(s)) {
                    break;
                }
            } else {
                if (Strings.isNullOrEmpty(s)) {
                    break;
                }
            }

            expectedResultBuilder.app(s).nl();
        } while (true);

        expectedPlan = expectedResultBuilder.toString().strip();
    }

    @Override
    void execute(ScriptContext ctx) {
        List<List<?>> res = ctx.executeQuery((mapping ? "EXPLAIN MAPPING FOR " : "EXPLAIN ") + sql);

        String actualPlan = (String) res.get(0).get(0);
        actualPlan = actualPlan.strip();
        if (!expectedPlan.equals(actualPlan)) {
            throw new AssertionError("Invalid plan at: " + posDesc + "." + System.lineSeparator()
                    + "Expected: " + System.lineSeparator() + expectedPlan + System.lineSeparator()
                    + "Actual: " + System.lineSeparator() + actualPlan);
        }
    }

    @Override
    public String toString() {
        return "Plan ["
                + "sql=" + sql
                + ", expectedPlan=" + expectedPlan
                + ']';
    }
}
