/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.deprecated;

import io.micronaut.context.ApplicationContext;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import org.apache.ignite.cli.commands.TopLevelCliCommand;
import picocli.CommandLine;

/**
 * Base class for CLI-related integration tests.
 */
public abstract class AbstractCliIntegrationTest extends AbstractCliTest {
    /** stderr. */
    protected final ByteArrayOutputStream err = new ByteArrayOutputStream();

    /** stdout. */
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();

    /**
     * Creates a new command line interpreter.
     *
     * @param applicationCtx DI context.
     * @return New command line instance.
     */
    protected final CommandLine cmd(ApplicationContext applicationCtx) {
        CommandLine.IFactory factory = new CommandFactory(applicationCtx);

        return new CommandLine(TopLevelCliCommand.class, factory)
                .setErr(new PrintWriter(err, true))
                .setOut(new PrintWriter(out, true));
    }
}
