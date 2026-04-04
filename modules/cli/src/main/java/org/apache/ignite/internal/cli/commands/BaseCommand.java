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

import static org.apache.ignite.internal.cli.commands.CommandConstants.ABBREVIATE_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.COMMAND_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.DESCRIPTION_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.HELP_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.OPTION_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.PARAMETER_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.REQUIRED_OPTION_MARKER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_OPTIONS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SYNOPSIS_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.USAGE_HELP_AUTO_WIDTH;
import static org.apache.ignite.internal.cli.commands.CommandConstants.VERBOSE_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_SHORT;

import org.apache.ignite.internal.cli.core.call.CallExecutionPipelineBuilder;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Base class for commands.
 */
@Command(
        descriptionHeading = DESCRIPTION_HEADING,
        optionListHeading = OPTION_LIST_HEADING,
        synopsisHeading = SYNOPSIS_HEADING,
        requiredOptionMarker = REQUIRED_OPTION_MARKER,
        usageHelpAutoWidth = USAGE_HELP_AUTO_WIDTH,
        sortOptions = SORT_OPTIONS,
        sortSynopsis = SORT_SYNOPSIS,
        abbreviateSynopsis = ABBREVIATE_SYNOPSIS,
        commandListHeading = COMMAND_LIST_HEADING,
        parameterListHeading = PARAMETER_LIST_HEADING
)
public abstract class BaseCommand {
    /** Help option specification. */
    @SuppressWarnings("unused")
    @Option(names = {HELP_OPTION, HELP_OPTION_SHORT}, description = HELP_OPTION_DESC, usageHelp = true, order = HELP_OPTION_ORDER)
    private boolean usageHelpRequested;

    /** Verbose option specification. */
    @Option(names = {VERBOSE_OPTION, VERBOSE_OPTION_SHORT}, description = VERBOSE_OPTION_DESC, order = VERBOSE_OPTION_ORDER)
    protected boolean[] verbose = new boolean[0];

    /** Instance of picocli command specification. */
    @Spec
    protected CommandSpec spec;

    /**
     * Sets output printers and verbosity flag and runs the pipeline.
     *
     * @param pipelineBuilder Pipeline builder.
     * @return Exit code.
     */
    protected <I extends CallInput, O> int runPipeline(CallExecutionPipelineBuilder<I, O> pipelineBuilder) {
        return pipelineBuilder
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .verbose(verbose)
                .build()
                .runPipeline();
    }

    /**
     * Sets verbosity flag and starts the flow.
     *
     * @param flowBuilder Flow builder.
     */
    protected <I, O> void runFlow(FlowBuilder<I, O> flowBuilder) {
        flowBuilder.verbose(verbose).start();
    }

    /**
     * Creates a {@link ClusterNotInitializedExceptionHandler} that suggests the appropriate init command based on whether this command is
     * running in REPL mode.
     *
     * @param message command-specific error text (e.g. "Cannot list units")
     */
    protected ClusterNotInitializedExceptionHandler createHandler(String message) {
        return new ClusterNotInitializedExceptionHandler(message, isReplMode() ? "cluster init" : "ignite cluster init");
    }

    /**
     * Detects whether this command is running in REPL mode by walking up to the root command and checking its class.
     */
    private boolean isReplMode() {
        CommandLine root = spec.commandLine();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        return root.getCommand() instanceof TopLevelCliReplCommand;
    }
}
