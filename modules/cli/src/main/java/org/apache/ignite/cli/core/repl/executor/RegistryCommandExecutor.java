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

package org.apache.ignite.cli.core.repl.executor;


import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.jline.console.SystemRegistry;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.Parser.ParseContext;

/**
 * Command executor based on {@link SystemRegistry}.
 */
public class RegistryCommandExecutor implements Call<StringCallInput, Object> {
    private final SystemRegistry systemRegistry;

    private final Parser parser;

    /**
     * Constructor.
     *
     * @param systemRegistry {@link SystemRegistry} instance.
     * @param parser A {@link Parser} used to create {@code systemRegistry}.
     */
    public RegistryCommandExecutor(SystemRegistry systemRegistry, Parser parser) {
        this.systemRegistry = systemRegistry;
        this.parser = parser;
    }

    /**
     * Executor method.
     *
     * @param input processed command line.
     * @return Command output.
     */
    @Override
    public CallOutput<Object> execute(StringCallInput input) {
        try {
            Object executionResult = systemRegistry.execute(input.getString());
            if (executionResult == null) {
                return DefaultCallOutput.empty();
            }

            return DefaultCallOutput.success(executionResult);
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    /**
     * Clean up {@link SystemRegistry}.
     */
    public void cleanUp() {
        systemRegistry.cleanUp();
    }

    /**
     * Determines whether the {@link SystemRegistry} has a command with this name.
     *
     * @param line command name to check.
     * @return true if the registry has the command.
     */
    public boolean hasCommand(String line) {
        ParsedLine pl = parser.parse(line, 0, ParseContext.SPLIT_LINE);

        return !pl.words().isEmpty() && systemRegistry.hasCommand(parser.getCommand(pl.words().get(0)));
    }
}
