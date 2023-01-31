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

package org.apache.ignite.internal.cli.core.repl.executor;


import java.util.ArrayList;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.jline.console.SystemRegistry;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.Parser.ParseContext;
import picocli.CommandLine;

/**
 * Command executor based on {@link SystemRegistry}.
 */
public class RegistryCommandExecutor implements Call<StringCallInput, Object> {

    private final Parser parser;

    private final CommandLine commandLine;

    /**
     * Constructor.
     *
     * @param parser A {@link Parser} used to create {@code systemRegistry}.
     * @param commandLine {@link CommandLine} instance.
     */
    public RegistryCommandExecutor(Parser parser, CommandLine commandLine) {
        this.parser = parser;
        this.commandLine = commandLine;
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
            String[] args = new ArrayList<>(parser.parse(input.getString(), 0, ParseContext.SPLIT_LINE).words())
                    .toArray(new String[0]);
            commandLine.execute(args);
            return DefaultCallOutput.empty();
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    /**
     * Clean up {@link SystemRegistry}.
     */
    public void cleanUp() {
        commandLine.clearExecutionResults();
    }

    /**
     * Determines whether the {@link SystemRegistry} has a command with this name.
     *
     * @param line command name to check.
     * @return true if the registry has the command.
     */
    public boolean hasCommand(String line) {
        ParsedLine pl = parser.parse(line, 0, ParseContext.SPLIT_LINE);

        return !pl.words().isEmpty() && commandLine.getSubcommands().containsKey(parser.getCommand(pl.words().get(0)));
    }
}
