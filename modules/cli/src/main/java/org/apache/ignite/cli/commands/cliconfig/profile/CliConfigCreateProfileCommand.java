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

package org.apache.ignite.cli.commands.cliconfig.profile;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cliconfig.profile.CliConfigCreateProfileCall;
import org.apache.ignite.cli.call.cliconfig.profile.CliConfigCreateProfileCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command for create CLI profile.
 */
@Command(name = "create", description = "Create profile command")
public class CliConfigCreateProfileCommand extends BaseCommand implements Callable<Integer> {
    @Option(names = {"--name", "-n"}, required = true, description = "Name of new profile")
    private String profileName;

    @Option(names = {"--copy-from", "-c"}, description = "Profile whose content will be copied to new one")
    private String copyFrom;

    @Option(names = {"--activate", "-a"}, description = "Activate new profile as current or not")
    private boolean activate;


    @Inject
    private CliConfigCreateProfileCall call;

    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(CliConfigCreateProfileCallInput.builder()
                        .profileName(profileName)
                        .copyFrom(copyFrom)
                        .activate(activate)::build)
                .errOutput(spec.commandLine().getErr())
                .output(spec.commandLine().getOut())
                .build().runPipeline();
    }
}
