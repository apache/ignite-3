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

import java.nio.file.Path;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.sql.IgniteSql;

/**
 * Runs one SQL test script.
 */
public class SqlScriptRunner {

    /**
     * Provides dependencies required for {@link SqlScriptRunner}.
     */
    public interface RunnerRuntime {

        /** Returns a query executor. **/
        IgniteSql sql();

        /** Returns a logger. **/
        IgniteLogger log();

        /**
         * Returns a name of the current database engine.
         * This property is used by {@code skipif} and {@code onlyif} commands.
         * **/
        String engineName();
    }

    /** Test script path. */
    private final Path test;

    /** Runtime dependencies of a script runner. **/
    private final RunnerRuntime runnerRuntime;

    /**
     * Script runner constructor.
     */
    public SqlScriptRunner(Path test, RunnerRuntime runnerRuntime) {
        this.test = test;
        this.runnerRuntime = runnerRuntime;
    }

    /**
     * Executes SQL file script.
     */
    public void run() throws Exception {
        var ctx = new ScriptContext(runnerRuntime);

        try (var script = new Script(test, ctx)) {
            for (Command cmd : script) {
                try {
                    cmd.execute(ctx);
                } catch (Exception e) {
                    throw script.reportError("Command execution error", cmd.getClass().getSimpleName(), e);
                } finally {
                    ctx.loopVars.clear();
                }
            }
        }
    }
}
