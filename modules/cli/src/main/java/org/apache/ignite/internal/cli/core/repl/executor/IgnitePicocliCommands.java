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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterRegistry;
import org.apache.ignite.internal.cli.core.repl.completer.filter.CompleterFilter;
import org.jline.builtins.Options.HelpException;
import org.jline.console.ArgDesc;
import org.jline.console.CmdDesc;
import org.jline.console.CommandRegistry;
import org.jline.reader.Candidate;
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

    /** Returns the {@link CommandLine} instance. */
    public CommandLine getCmd() {
        return cmd;
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
        Help cmdhelp = new Help(spec);
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
                key += "=" + o.paramLabel(); // NOPMD
            }
            options.put(key, val);
        }
        return new CmdDesc(main, ArgDesc.doArgNames(List.of("")), options);
    }

    /** {@inheritDoc} */
    @Override
    public List<String> commandInfo(String command) {
        CommandSpec spec = cmd.getSubcommands().get(command).getCommandSpec();
        Help cmdhelp = new Help(spec);
        String description = AttributedString.stripAnsi(spec.usageMessage().sectionMap().get("description").render(cmdhelp));
        return new ArrayList<>(Arrays.asList(description.split("\\r?\\n")));
    }

    @Override
    public Object invoke(CommandRegistry.CommandSession session, String command, Object[] args) {
        List<String> arguments = new ArrayList<>();
        arguments.add(command);
        Arrays.stream(args).map(Object::toString).forEach(arguments::add);
        cmd.execute(arguments.toArray(new String[0]));
        return null;
    }

    public Object executeHelp(Object[] args) {
        return invoke(new CommandSession(), "help", args);
    }

    @Override
    public Set<String> commandNames() {
        return commands;
    }

    @Override
    public Map<String, String> commandAliases() {
        return aliasCommand;
    }

    private class IgnitePicocliCompleter extends ArgumentCompleter {

        public IgnitePicocliCompleter() {
            super(NullCompleter.INSTANCE);
        }

        @Override
        public void complete(LineReader reader, ParsedLine commandLine, List<Candidate> candidates) {
            assert commandLine != null;
            assert candidates != null;

            // let picocli generate completion candidates for the token where the cursor is at
            String[] words = commandLine.words().toArray(new String[0]);
            List<CharSequence> cs = new ArrayList<>();
            AutoComplete.complete(cmd.getCommandSpec(),
                    words,
                    commandLine.wordIndex(),
                    0,
                    commandLine.cursor(),
                    cs);

            try {
                completerRegistry.findCompleters(words).stream()
                        .map(c -> c.complete(words))
                        .flatMap(List::stream)
                        .map(this::dynamicCandidate)
                        .forEach(candidates::add);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (candidates.isEmpty()) {
                String[] filteredCandidates = cs.stream()
                        .map(CharSequence::toString)
                        .toArray(String[]::new);
                for (CompleterFilter filter : completerFilters) {
                    filteredCandidates = filter.filter(words, filteredCandidates);
                }

                for (String c : filteredCandidates) {
                    candidates.add(staticCandidate(c));
                }
            }

            sortCandidates(candidates);
        }

        private Candidate dynamicCandidate(String one) {
            // more dots means deeper level of the config tree, so we don't want to show them in the completion at first positions
            // the absence of dots, on the other hand, means that this completion should have higher sorting priority
            int sortingPriority = one.split("\\.").length;

            return new Candidate(one, one, null, null, null, null, false, sortingPriority);
        }

        private Candidate staticCandidate(String one) {
            return new Candidate(one, one, null, null, null, null, true, 10);
        }

        /**
         * When custom sort order is used, sort candidates list and reassign sort order according to candidates order.
         * TODO https://issues.apache.org/jira/browse/IGNITE-21824
         *
         * @param candidates List of candidates.
         */
        private void sortCandidates(List<Candidate> candidates) {
            boolean customOrder = candidates.stream().anyMatch(c -> c.sort() != 0);
            if (!customOrder) {
                return;
            }
            Collections.sort(candidates);
            List<Candidate> newCandidates = new ArrayList<>(candidates.size());
            for (int i = 0; i < candidates.size(); i++) {
                Candidate candidate = candidates.get(i);
                Candidate newCandidate = new Candidate(
                        candidate.value(),
                        candidate.displ(),
                        candidate.group(),
                        candidate.descr(),
                        candidate.suffix(),
                        candidate.key(),
                        candidate.complete(),
                        i // override sort order
                );
                newCandidates.add(newCandidate);
            }
            candidates.clear();
            candidates.addAll(newCandidates);
        }
    }
}
