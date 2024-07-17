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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Loop command allows to perform a block of commands multiple times:
 * <pre>
 *     # variable is an integer
 *     loop variable start-value end-value-exclusive
 * </pre>
 * When an SQL statement or an SQL query is executed inside a loop, all placeholders {@code ${variable}}
 * in that statement/query are replaced with the current value of that {@code variable}.
 * <pre>
 *     # Executes the query 10 times with i=[0, 2)
 *     loop i 0 2
 *     query I
 *     SELECT MIN(c1) FROM (VALUES(${i}), (-1)) t(c1);
 *     ----
 *     -1
 *     # A loop must be terminated by a EndLoop command.
 *     endloop
 *
 *     # The above loop is equivalent to two query commands:
 *     query I
 *     SELECT MIN(c1) FROM (VALUES(0), (-1)) t(c1);
 *     ----
 *     -1
 *
 *     query I
 *     SELECT MIN(c1) FROM (VALUES(1), (-1)) t(c1);
 *     ----
 *     -1
 * </pre>
 *
 * @see EndLoop endloop command
 */
final class Loop extends Command {

    private final List<Command> cmds = new ArrayList<>();

    private final int begin;

    private final int end;

    private final String var;

    Loop(Script script, ScriptContext ctx, String[] cmdTokens) throws IOException {
        super(script.scriptPosition());

        try {
            var = cmdTokens[1];
            begin = Integer.parseInt(cmdTokens[2]);
            end = Integer.parseInt(cmdTokens[3]);
        } catch (Exception e) {
            throw script.reportInvalidCommand("Unexpected loop syntax", cmdTokens, e);
        }

        while (script.ready()) {
            Command cmd = script.nextCommand();
            if (cmd instanceof EndLoop) {
                break;
            }

            cmds.add(cmd);
        }
    }

    @Override
    void execute(ScriptContext ctx) {
        for (int i = begin; i < end; ++i) {
            ctx.loopVars.put(var, i);

            for (Command cmd : cmds) {
                cmd.execute(ctx);
            }
        }
    }
}
