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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.core.repl.completer.DynamicCompleter;
import org.apache.ignite.cli.core.repl.completer.DynamicCompleterRegistry;
import org.apache.ignite.cli.core.repl.completer.HoconDynamicCompleter;
import org.jetbrains.annotations.NotNull;
import org.jline.builtins.Options.HelpException;
import org.jline.console.ArgDesc;
import org.jline.console.CmdDesc;
import org.jline.console.CommandRegistry;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.SystemCompleter;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.InfoCmp.Capability;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.shell.jline3.PicocliCommands;

/**
 * Inspired by {@link PicocliCommands} but with custom completer.
 */
public class IgnitePicocliCommands implements CommandRegistry {

    private final CommandLine cmd;
    private final Set<String> commands;
    private final Map<String, String> aliasCommand = new HashMap<>();
    private final PicocliCommands.PicocliCommandsFactory factory;
    private final DynamicCompleterRegistry completerRegistry;

    public IgnitePicocliCommands(
            CommandLine cmd,
            PicocliCommands.PicocliCommandsFactory factory,
            DynamicCompleterRegistry completerRegistry) {

        this.cmd = cmd;
        commands = cmd.getCommandSpec().subcommands().keySet();
        for (String c : commands) {
            for (String a : cmd.getSubcommands().get(c).getCommandSpec().aliases()) {
                aliasCommand.put(a, c);
            }
        }
        this.factory = factory;
        this.completerRegistry = completerRegistry;
    }

    /**
     * @param command
     * @return true if PicocliCommands contains command
     */
    public boolean hasCommand(String command) {
        return commands.contains(command) || aliasCommand.containsKey(command);
    }

    public SystemCompleter compileCompleters() {
        SystemCompleter out = new SystemCompleter();
        List<String> all = new ArrayList<>();
        all.addAll(commands);
        all.addAll(aliasCommand.keySet());
        out.add(all, new PicocliCompleter());
        return out;
    }

    private CommandLine findSubcommandLine(List<String> args, int lastIdx) {
        CommandLine out = cmd;
        for (int i = 0; i < lastIdx; i++) {
            if (!args.get(i).startsWith("-")) {
                out = findSubcommandLine(out, args.get(i));
                if (out == null) {
                    break;
                }
            }
        }
        return out;
    }

    private CommandLine findSubcommandLine(CommandLine cmdline, String command) {
        for (CommandLine s : cmdline.getSubcommands().values()) {
            if (s.getCommandName().equals(command) || Arrays.asList(s.getCommandSpec().aliases()).contains(command)) {
                return s;
            }
        }
        return null;
    }

    /**
     * @param args
     * @return command description for JLine TailTipWidgets to be displayed in terminal status bar.
     */
    @Override
    public CmdDesc commandDescription(List<String> args) {
        CommandLine sub = findSubcommandLine(args, args.size());
        if (sub == null) {
            return null;
        }
        CommandSpec spec = sub.getCommandSpec();
        Help cmdhelp = new picocli.CommandLine.Help(spec);
        List<AttributedString> main = new ArrayList<>();
        Map<String, List<AttributedString>> options = new HashMap<>();
        String synopsis = AttributedString.stripAnsi(spec.usageMessage().sectionMap().get("synopsis").render(cmdhelp));
        main.add(HelpException.highlightSyntax(synopsis.trim(), HelpException.defaultStyle()));
        // using JLine help highlight because the statement below does not work well...
        //        main.add(new AttributedString(spec.usageMessage().sectionMap().get("synopsis").render(cmdhelp).toString()));
        for (OptionSpec o : spec.options()) {
            String key = Arrays.stream(o.names()).collect(Collectors.joining(" "));
            List<AttributedString> val = new ArrayList<>();
            for (String d : o.description()) {
                val.add(new AttributedString(d));
            }
            if (o.arity().max() > 0) {
                key += "=" + o.paramLabel();
            }
            options.put(key, val);
        }
        return new CmdDesc(main, ArgDesc.doArgNames(List.of("")), options);
    }

    @Override
    public List<String> commandInfo(String command) {
        List<String> out = new ArrayList<>();
        CommandSpec spec = cmd.getSubcommands().get(command).getCommandSpec();
        Help cmdhelp = new picocli.CommandLine.Help(spec);
        String description = AttributedString.stripAnsi(spec.usageMessage().sectionMap().get("description").render(cmdhelp));
        out.addAll(Arrays.asList(description.split("\\r?\\n")));
        return out;
    }

    // For JLine >= 3.16.0
    @Override
    public Object invoke(CommandRegistry.CommandSession session, String command, Object[] args) throws Exception {
        List<String> arguments = new ArrayList<>();
        arguments.add(command);
        arguments.addAll(Arrays.stream(args).map(Object::toString).collect(Collectors.toList()));
        cmd.execute(arguments.toArray(new String[0]));
        return null;
    }

    // @Override This method was removed in JLine 3.16.0; keep it in case this component is used with an older version of JLine
    public Object execute(CommandRegistry.CommandSession session, String command, String[] args) throws Exception {
        List<String> arguments = new ArrayList<>();
        arguments.add(command);
        arguments.addAll(Arrays.asList(args));
        cmd.execute(arguments.toArray(new String[0]));
        return null;
    }

    @Override
    public Set<String> commandNames() {
        return commands;
    }

    @Override
    public Map<String, String> commandAliases() {
        return aliasCommand;
    }

    // @Override This method was removed in JLine 3.16.0; keep it in case this component is used with an older version of JLine
    public CmdDesc commandDescription(String command) {
        return null;
    }

    /**
     * Command that clears the screen.
     * <p>
     * <b>WARNING:</b> This subcommand needs a JLine {@code Terminal} to clear the screen.
     * To accomplish this, construct the {@code CommandLine} with a {@code PicocliCommandsFactory}, and set the {@code Terminal} on that
     * factory. For example:
     * <pre>
     * &#064;Command(subcommands = PicocliCommands.ClearScreen.class)
     * class MyApp //...
     *
     * PicocliCommandsFactory factory = new PicocliCommandsFactory();
     * CommandLine cmd = new CommandLine(new MyApp(), factory);
     * // create terminal
     * factory.setTerminal(terminal);
     * </pre>
     *
     * @since 4.6
     */
    @Command(name = "cls", aliases = "clear", mixinStandardHelpOptions = true,
            description = "Clears the screen", version = "1.0")
    public static class ClearScreen implements Callable<Void> {

        private final Terminal terminal;

        ClearScreen(Terminal terminal) {
            this.terminal = terminal;
        }

        public Void call() throws IOException {
            if (terminal != null) {
                terminal.puts(Capability.clear_screen);
            }
            return null;
        }
    }

    private class PicocliCompleter extends ArgumentCompleter implements Completer {

        public PicocliCompleter() {
            super(NullCompleter.INSTANCE);
        }

        @Override
        public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
            assert commandLine != null;
            assert candidates != null;
            // let picocli generate completion candidates for the token where the cursor is at
            final String[] words = commandLine.words().toArray(new String[0]);
            List<CharSequence> cs = new ArrayList<CharSequence>();
            AutoComplete.complete(cmd.getCommandSpec(),
                    words,
                    commandLine.wordIndex(),
                    0,
                    commandLine.cursor(),
                    cs);

            if (cs.size() > 0) {
                for (CharSequence c : cs) {
                    candidates.add(new Candidate(c.toString()));
                }
            } else {
                List<DynamicCompleter> completers = completerRegistry.findCompleters(words);
                if (completers.size() > 0) {
                    try {
                        completers.stream()
                                .map(c -> c.complete(words))
                                .flatMap(List::stream)
                                .map(this::candidate)
                                .forEach(candidates::add);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @NotNull
        private Candidate candidate(String one) {
            // more dots means deeper level of the config tree, so we don't want to show them in the completion at firs positions
            // the epson of dots wise-versa wanted to be placed at the first position
            int sortingPriority = one.split("\\.").length;

            return new Candidate(one, one, null, null, null, null, false, sortingPriority);
        }

        private void addCandidates(List<Candidate> candidates, Iterable<String> cands) {
            addCandidates(candidates, cands, "", "", true);
        }

        private void addCandidates(List<Candidate> candidates, Iterable<String> cands, String preFix, String postFix, boolean complete) {
            for (String s : cands) {
                candidates.add(new Candidate(AttributedString.stripAnsi(preFix + s + postFix), s, null, null, null, null, complete));
            }
        }

    }
}
