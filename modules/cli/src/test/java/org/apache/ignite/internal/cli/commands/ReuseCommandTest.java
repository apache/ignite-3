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

package org.apache.ignite.internal.cli.commands;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Tests that user objects are not reused if they are not singletons.
 */
@MicronautTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class ReuseCommandTest {
    @Inject
    private ApplicationContext context;

    private CommandLine cmd;

    private StringWriter sout;

    @BeforeEach
    public void setUp() {
        cmd = new CommandLine(Command.class, new MicronautFactory(context));
        sout = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
    }

    @Test
    public void test1() {
        cmd.execute("--option", "option");
        Assertions.assertEquals("option", sout.toString());
    }

    @Test
    public void test2() {
        cmd.execute();
        Assertions.assertEquals("null", sout.toString());
    }

    @CommandLine.Command(name = "command")
    static class Command implements Runnable {
        @Option(names = "--option")
        private String option;

        @Spec
        CommandSpec spec;

        @Override
        public void run() {
            spec.commandLine().getOut().print(option);
            spec.commandLine().getOut().flush();
        }
    }
}
