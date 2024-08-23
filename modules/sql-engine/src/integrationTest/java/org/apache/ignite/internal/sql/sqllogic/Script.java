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

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.Nullable;

/**
 * Script contains a list of commands.
 */
final class Script implements Iterable<Command>, AutoCloseable {

    /**
     * Line separator to bytes representation.
     * NB: Don't use {@code System.lineSeparator()} here.
     * The separator is used to calculate hash by SQL results and mustn't be changed on the different platforms.
     */
    static final byte[] LINE_SEPARATOR_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

    /** Supported commands. **/
    private static final Map<String, CommandParser> COMMANDS = Map.of(
            "statement", Statement::new,
            "query", Query::new,
            "loop", Loop::new,
            "endloop", EndLoop::new,
            "for", For::new,
            "endfor", EndFor::new,
            "skipif", SkipIf::new,
            "onlyif", OnlyIf::new
    );

    /** A file. **/
    private final String fileName;

    private final BufferedReader buffReader;

    /** The current line in a script .**/
    private int lineNum;

    /** A script execution context. **/
    private final ScriptContext ctx;

    Script(Path test, ScriptContext ctx) throws IOException {
        this.fileName = test.getFileName().toString();
        this.buffReader = Files.newBufferedReader(test);
        this.ctx = ctx;
    }

    String nextLine() throws IOException {
        return nextLineWithoutTrim();
    }

    String nextLineWithoutTrim() throws IOException {
        String s = buffReader.readLine();

        lineNum++;

        return s;
    }

    boolean ready() throws IOException {
        return buffReader.ready();
    }

    ScriptPosition scriptPosition() {
        return new ScriptPosition(fileName, lineNum);
    }

    @Override
    public void close() throws Exception {
        buffReader.close();
    }

    @Nullable Command nextCommand() {
        try {
            while (ready()) {
                String line = nextLine();

                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
                    continue;
                }

                String[] tokens = line.split("\\s+");
                if (tokens.length == 0) {
                    throw reportError("Invalid line", line);
                }

                String token = tokens[0];
                return parseCommand(token, tokens, line);
            }

            return null;
        } catch (IOException e) {
            throw reportError("Can not read next command", null, e);
        }
    }

    private Command parseCommand(String token, String[] tokens, String line) throws IOException {
        CommandParser parser = COMMANDS.get(token);
        if (parser == null) {
            throw reportError("Unexpected command " + token, line);
        }
        try {
            return parser.parse(this, ctx, tokens);
        } catch (Exception e) {
            throw reportError("Failed to parse a command", line, e);
        }
    }

    @Override
    public Iterator<Command> iterator() {
        Command cmd0 = nextCommand();
        return new Iterator<>() {

            private @Nullable Command cmd = cmd0;

            @Override
            public boolean hasNext() {
                return cmd != null;
            }

            @Override
            public Command next() {
                if (cmd == null) {
                    throw new NoSuchElementException();
                }

                Command ret = cmd;

                cmd = nextCommand();

                return ret;
            }
        };
    }

    ScriptException reportInvalidCommand(String error, String[] command) {
        var pos = scriptPosition();

        return new ScriptException(error, pos, Arrays.toString(command));
    }

    ScriptException reportInvalidCommand(String error, String[] command, @Nullable Throwable cause) {
        var pos = scriptPosition();

        return new ScriptException(error, pos, Arrays.toString(command), cause);
    }

    ScriptException reportError(String error, @Nullable String message) {
        var pos = scriptPosition();

        return new ScriptException(error, pos, message);
    }

    ScriptException reportError(String error, @Nullable String message, @Nullable Throwable cause) {
        var pos = scriptPosition();

        return new ScriptException(error, pos, message, cause);
    }

    /**
     * Parses tokens and produces a command.
     */
    @FunctionalInterface
    static interface CommandParser {

        /**
         * Parses the given array of tokens and produces a {@link Command}.
         *
         * @param script  a script.
         * @param ctx  a script context,
         * @param tokens an array of tokens. the first token in the array is the type of this command.
         *
         * @return an instance of a command.
         * @throws IOException when i/o error ocurrs.
         */
        Command parse(Script script, ScriptContext ctx, String[] tokens) throws IOException;
    }
}
