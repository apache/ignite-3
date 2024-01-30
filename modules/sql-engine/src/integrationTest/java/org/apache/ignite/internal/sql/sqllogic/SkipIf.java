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

/**
 * SkipIf command skips the next command in a script if the specific condition does not hold.
 * <pre>
 *     skipif condition
 *     statement ok
 *     SELECT broken
 * </pre>
 * At the moment the only supported condition type is a name of a database engine.
 */
final class SkipIf extends Command {

    private final String[] condition;

    private final Command command;

    SkipIf(Script script, ScriptContext ctx, String[] tokens) {
        super(script.scriptPosition());

        var nextCommand = script.nextCommand();
        if (nextCommand == null) {
            throw script.reportInvalidCommand("Expected a next command", tokens);
        }

        this.command = nextCommand;
        this.condition = tokens;
    }

    @Override
    void execute(ScriptContext ctx) {
        if (condition[1].equals(ctx.engineName)) {
            return;
        }
        command.execute(ctx);
    }
}
