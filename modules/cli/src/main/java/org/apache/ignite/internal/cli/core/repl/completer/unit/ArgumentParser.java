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

package org.apache.ignite.internal.cli.core.repl.completer.unit;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

/** Parses typed words. */
@Singleton
public class ArgumentParser {

    private final CommandLine commandLine;

    public ArgumentParser() {
        commandLine = new CommandLine(TopLevelCliReplCommand.class);
    }

    /**
     * Parses first positional parameter. ["unit", "deploy", "my.first", "--version"] -> "my.first".
     *
     * @param words Words.
     * @return First positional parameter or {@code null} if it is not found.
     */
    @Nullable
    public String parseFirstPositionalParameter(String[] words) {
        try {
            ParseResult parseResult = commandLine.parseArgs(words);
            while (parseResult.hasSubcommand()) {
                parseResult = parseResult.subcommand();
            }

            return parseResult.matchedArgs().get(0).getValue();
        } catch (Exception e) {
            return null;
        }
    }
}
