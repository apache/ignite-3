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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * For command allows to perform a block of commands multiple times:
 * <pre>
 *     for variable in [ value1, value2, ..., valueN ]
 * </pre>
 * When an SQL statement or an SQL query is executed inside a for loop, all placeholders {@code ${variable}}
 * in that statement/query including results and error messages are replaced with the current value of that {@code variable}.
 * <pre>
 *     # Executes the query 2 times with i=0 and i=2
 *     for k in [0, 2]
 *     query I
 *     SELECT c1 FROM (VALUES (${i})) t(c1)
 *     ----
 *     ${i}
 *     # A for loop must be terminated by a EndLoop command.
 *     endfor
 *
 *     # The above loop is equivalent to two query commands:
 *     query I
 *     SELECT c1 FROM (VALUES (0)) t(c1);
 *     ----
 *     0
 *
 *     query I
 *     SELECT MIN(c1) FROM (VALUES(2), (-1)) t(c1);
 *     ----
 *     2
 * </pre>
 * Additional examples:
 * <pre>
 *     # variable is one or multiple words
 *     for var in [TEXT 1, TEXT 2, TEXT 3]
 *     ...
 *     endfor
 *
 *     # it is possible to escape a comma symbol:
 *     for var in [DECIMAL(2\,1)]
 *     ...
 *     endfor
 * </pre>
 *
 * @see EndFor endfor command
 */
final class For extends Command {

    private final List<Command> cmds = new ArrayList<>();

    private final List<String> elements;

    private final String var;

    For(Script script, ScriptContext ctx, String[] cmdTokens) throws IOException {
        super(script.scriptPosition());

        try {
            var = cmdTokens[1];

            // Parses tokens 'a,' 'b,' 'c' into a list of strings ['a', 'b', 'c' ]
            StringBuilder current = new StringBuilder();

            String inTok = cmdTokens[2];
            if (!"in".equalsIgnoreCase(inTok))  {
                throw script.reportInvalidCommand("Unexpected for syntax. Expected IN but got " + inTok, cmdTokens);
            }

            String leftBracket = cmdTokens[3];
            if (!"[".equalsIgnoreCase(leftBracket) && !leftBracket.startsWith("["))  {
                throw script.reportInvalidCommand("Unexpected for syntax. Expected [ but got " + leftBracket, cmdTokens);
            }

            String rightBracket = cmdTokens[cmdTokens.length - 1];
            if (!"]".equalsIgnoreCase(leftBracket) && !rightBracket.endsWith("]"))  {
                throw script.reportInvalidCommand("Unexpected for syntax. Expected ] but got " + rightBracket, cmdTokens);
            }

            elements = new ArrayList<>();

            for (int i = 3, elem = 0; i < cmdTokens.length; i++, elem++) {
                String tok = cmdTokens[i];

                // if the first token start with [vv, remove [
                // If the last token ends with vv], we should remove ] .
                if (elem == 0 && tok.startsWith("[")) {
                    if (i + 1 == cmdTokens.length && tok.endsWith("]")) {
                        current.append(tok, 1, tok.length() - 1);
                    } else {
                        current.append(tok.substring(1));
                    }
                } else if (i + 1 == cmdTokens.length && tok.endsWith("]")) {
                    current.append(tok, 0, tok.length() - 1);
                } else {
                    current.append(tok);
                }

                if (tok.endsWith(",")) {
                    String str = current.toString();
                    String e = str.substring(0, str.length() - 1).trim();

                    if (!e.isEmpty()) {
                        // Unescape comma
                        elements.add(e.replace("\\,", ","));
                    } else {
                        String error = format("Unexpected for syntax. For loop element can not be empty. Index: {}", elem);
                        throw script.reportInvalidCommand(error, cmdTokens);
                    }

                    current.setLength(0);
                } else {
                    current.append(' ');
                }
            }

            if (current.length() > 0) {
                String e = current.toString().trim();
                if (!e.isEmpty()) {
                    elements.add(e);
                }
            }
        } catch (Exception e) {
            throw script.reportInvalidCommand("Unexpected loop syntax", cmdTokens, e);
        }

        while (script.ready()) {
            Command cmd = script.nextCommand();
            if (cmd instanceof EndFor) {
                break;
            }

            cmds.add(cmd);
        }
    }

    @Override
    void execute(ScriptContext ctx) {
        for (String elem : elements) {
            ctx.loopVars.put(var, elem);

            for (Command cmd : cmds) {
                cmd.execute(ctx);
            }
        }
    }
}
