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

package org.apache.ignite.internal.cli.commands.cliconfig.profile;

import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_ACTIVATE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_ACTIVATE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_COPY_FROM_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_COPY_FROM_OPTION_DESC;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cliconfig.profile.CliConfigProfileCreateCall;
import org.apache.ignite.internal.cli.call.cliconfig.profile.CliConfigProfileCreateCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command for create CLI profile.
 */
@Command(name = "create", description = "Creates profile")
public class CliConfigProfileCreateCommand extends BaseCommand implements Callable<Integer> {
    @Parameters(arity = "1", description = "Name of new profile")
    private String profileName;

    @Option(names = PROFILE_COPY_FROM_OPTION, description = PROFILE_COPY_FROM_OPTION_DESC)
    private String copyFrom;

    @Option(names = PROFILE_ACTIVATE_OPTION, description = PROFILE_ACTIVATE_OPTION_DESC)
    private boolean activate;

    @Inject
    private CliConfigProfileCreateCall call;

    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(CliConfigProfileCreateCallInput.builder()
                        .profileName(profileName)
                        .copyFrom(copyFrom)
                        .activate(activate)::build)
                .errOutput(spec.commandLine().getErr())
                .output(spec.commandLine().getOut())
                .verbose(verbose)
                .build().runPipeline();
    }
}
