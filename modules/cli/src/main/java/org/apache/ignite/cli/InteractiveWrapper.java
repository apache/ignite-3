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

package org.apache.ignite.cli;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.keymap.KeyMap;
import org.jline.reader.Binding;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.Reference;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;

public class InteractiveWrapper {

    public void run(CommandLine cmd) {
        PicocliCommands picocliCommands = new PicocliCommands(workDir(), cmd) {
            @Override public Object invoke(CommandSession ses, String cmd, Object... args) throws Exception {
                return execute(ses, cmd, (String[])args);
            }
        };

        Parser parser = new DefaultParser();
        try (Terminal terminal = TerminalBuilder.builder().build()) {
            SystemRegistry systemRegistry = new SystemRegistryImpl(parser, terminal, InteractiveWrapper::workDir, null);
            systemRegistry.setCommandRegistries(picocliCommands);

            LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(systemRegistry.completer())
                .parser(parser)
                .variable(LineReader.LIST_MAX, 50)   // max tab completion candidates
                .build();

            TailTipWidgets widgets = new TailTipWidgets(reader, systemRegistry::commandDescription, 5, TailTipWidgets.TipType.COMPLETER);
            widgets.enable();

            KeyMap<Binding> keyMap = reader.getKeyMaps().get("main");
            keyMap.bind(new Reference("tailtip-toggle"), KeyMap.alt("s"));

            String prompt = "ignite> ";
            String rightPrompt = null;

            String line;
            while (true) {
                try {
                    systemRegistry.cleanUp();

                    line = reader.readLine(prompt, rightPrompt, (MaskingCallback) null, null);

                    systemRegistry.execute(line);
                } catch (UserInterruptException ignored) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                } catch (Exception e) {
                    systemRegistry.trace(e);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Path workDir() {
        return Paths.get(System.getProperty("user.dir"));
    }
}
