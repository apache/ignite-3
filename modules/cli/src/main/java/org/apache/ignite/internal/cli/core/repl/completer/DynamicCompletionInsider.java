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

package org.apache.ignite.internal.cli.core.repl.completer;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cli.commands.TopLevelCliReplCommand;
import picocli.CommandLine;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.ParseResult;

/** Provides insight into dynamic completion. */
public class DynamicCompletionInsider {

    private final CommandLine commandLine;

    public DynamicCompletionInsider() {
        commandLine = new CommandLine(TopLevelCliReplCommand.class);
    }

    private static String[] trim(String[] typedWords) {
        List<String> trimmedWords = new ArrayList<>(typedWords.length);
        int offset = 0;
        for (String typedWord : typedWords) {
            String currentTrim = typedWord.trim();
            if (currentTrim.isEmpty()) {
                offset++;
            } else {
                trimmedWords.add(currentTrim);
            }
        }
        return trimmedWords.toArray(new String[typedWords.length - offset]);
    }

    /** Say if the positional parameter is already completed. */
    public boolean wasPositionalParameterCompleted(String[] typedWords) {
        try {
            String[] trimmedWords = trim(typedWords);

            ParseResult parseResult = commandLine.parseArgs(trimmedWords);
            while (parseResult.hasSubcommand()) {
                parseResult = parseResult.subcommand();
            }

            boolean hasPositional = parseResult.hasMatchedPositional(0);
            boolean wasTrimmed = trimmedWords.length == typedWords.length;
            String matchedPositional = parseResult.matchedPositionals().get(0).originalStringValues().get(0);
            boolean lastWordIsPositional = matchedPositional.equals(trimmedWords[trimmedWords.length - 1]);
            boolean positionalTyping = wasTrimmed && hasPositional && lastWordIsPositional;

            return !positionalTyping && hasPositional;

        } catch (MissingParameterException e) {
            return e.getMissing().stream().noneMatch(ArgSpec::isPositional);
        } catch (Exception e) {
            return false; // better to return false than to throw an exception
        }
    }
}
