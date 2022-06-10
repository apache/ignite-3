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

package org.apache.ignite.cli.commands;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import picocli.CommandLine;

/**
 * Base class for testing CLI commands.
 */
@MicronautTest
public class CliCommandTestBase {
    @Inject
    private ApplicationContext context;

    private CommandLine commandLine;

    protected StringWriter err;

    protected StringWriter out;

    private int exitCode = Integer.MIN_VALUE;

    protected void setUp(Class<?> commandClass) {
        err = new StringWriter();
        out = new StringWriter();
        commandLine = new CommandLine(commandClass, new MicronautFactory(context));
        commandLine.setErr(new PrintWriter(err));
        commandLine.setOut(new PrintWriter(out));
    }

    protected void execute(String... args) {
        exitCode = commandLine.execute(args);
    }
}
