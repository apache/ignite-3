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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cli.core.repl.completer.CompleterFilter;
import org.apache.ignite.cli.core.repl.completer.DynamicCompleter;
import org.apache.ignite.cli.core.repl.completer.DynamicCompleterRegistry;
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
import org.jline.utils.AttributedString;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Help;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.shell.jline3.PicocliCommands;

/**
 * Inspired by {@link PicocliCommands} but with custom dynamic completer and completer filtering.
 */
public class IgnitePicocliCommands implements CommandRegistry {

    private final CommandLine cmd;
    private final Set<String> commands;
    private final Map<String, String> aliasCommand = new HashMap<>();
    private final DynamicCompleterRegistry completerRegistry;
    private final List<CompleterFilter> completerFilters;

    /** Default constructor. */
    public IgnitePicocliCommands(CommandLine cmd, DynamicCompleterRegistry completerRegistry, List<CompleterFilter> completerFilters) {
        this.cmd = cmd;
        this.completerFilters = completerFilters;
        this.completerRegistry = completerRegistry;
        this.commands = cmd.getCommandSpec().subcommands().keySet();
        putAliases(cmd);
    }

    private void putAliases(CommandLine cmd) {
        for (String c : commands) {
            for (String a : cmd.getSubcommands().get(c).getCommandSpec().aliases()) {
                aliasCommand.put(a, c);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasCommand(String command) {
        return commands.contains(command) || aliasCommand.containsKey(command);
    }

    /** {@inheritDoc} */
    @Override
    public SystemCompleter compileCompleters() {
        SystemCompleter out = new SystemCompleter();
        List<String> all = new ArrayList<>();
        all.addAll(commands);
        all.addAll(aliasCommand.keySet());
        out.add(all, new IgnitePicocliCompleter());
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

    /** {@inheritDoc} */
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
            String key = String.join(" ", o.names());
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

    /** {@inheritDoc} */
    @Override
    public List<String> commandInfo(String command) {
        CommandSpec spec = cmd.getSubcommands().get(command).getCommandSpec();
        Help cmdhelp = new picocli.CommandLine.Help(spec);
        String description = AttributedString.stripAnsi(spec.usageMessage().sectionMap().get("description").render(cmdhelp));
        return new ArrayList<>(Arrays.asList(description.split("\\r?\\n")));
    }

    @Override
    public Object invoke(CommandRegistry.CommandSession session, String command, Object[] args) throws Exception {
        List<String> arguments = new ArrayList<>();
        arguments.add(command);
        Arrays.stream(args).map(Object::toString).forEach(arguments::add);
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

    private class IgnitePicocliCompleter extends ArgumentCompleter implements Completer {

        public IgnitePicocliCompleter() {
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

            List<String> staticCandidates = new ArrayList<>();
            for (CharSequence c : cs) {
                staticCandidates.add(c.toString());
            }

            List<DynamicCompleter> completers = completerRegistry.findCompleters(words);
            if (completers.size() > 0) {
                try {
                    completers.stream()
                            .map(c -> c.complete(words))
                            .flatMap(List::stream)
                            .map(this::dynamicCandidate)
                            .forEach(candidates::add);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            String[] filteredCandidates = completerFilters.get(0).filter(words, staticCandidates.toArray(String[]::new));
            for (int i = 1; i < completerFilters.size(); i++) {
                filteredCandidates = completerFilters.get(i).filter(words, filteredCandidates);
            }

            for (String c : filteredCandidates) {
                candidates.add(new Candidate(c));
            }
        }

        private Candidate dynamicCandidate(String one) {
            // more dots means deeper level of the config tree, so we don't want to show them in the completion at first positions
            // the absence of dots, on the other hand, means that this completion should have higher sorting priority
            int sortingPriority = one.split("\\.").length;

            return new Candidate(one, one, null, null, null, null, false, sortingPriority);
        }
    }
}
