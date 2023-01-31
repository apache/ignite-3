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

package org.apache.ignite.internal.cli;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.junit.jupiter.api.BeforeAll;
import picocli.CommandLine;

/**
 * Base class for any CLI tests.
 */
@MicronautTest(rebuildContext = true)
public abstract class AbstractCliTest {
    @Inject
    private ApplicationContext ctx;

    /** stderr. */
    protected ByteArrayOutputStream err = new ByteArrayOutputStream();

    /** stdout. */
    protected ByteArrayOutputStream out = new ByteArrayOutputStream();

    /**
     * Creates a new command line interpreter.
     *
     * @return New command line instance.
     */
    private CommandLine cmd() {
        return new CommandLine(TopLevelCliCommand.class, new MicronautFactory(ctx))
                .setErr(new PrintWriter(err, true))
                .setOut(new PrintWriter(out, true));
    }

    protected final int execute(String... args) {
        return cmd().execute(args);
    }

    /**
     * Sets up a dumb terminal before tests.
     */
    @BeforeAll
    static void beforeAll() {
        System.setProperty("org.jline.terminal.dumb", "true");
    }

    /**
     * Reset stderr and stdout streams.
     */
    protected void resetStreams() {
        err.reset();
        out.reset();
    }
}
