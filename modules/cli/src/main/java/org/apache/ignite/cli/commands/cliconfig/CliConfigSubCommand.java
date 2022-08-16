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

package org.apache.ignite.cli.commands.cliconfig;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.call.cliconfig.CliConfigCall;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.cliconfig.profile.CliConfigProfileCommand;
import org.apache.ignite.cli.commands.decorators.ProfileDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.StringCallInput;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Parent command for CLI configuration commands.
 */
@Command(name = "config", subcommands = {
        CliConfigGetSubCommand.class,
        CliConfigSetSubCommand.class,
        CliConfigProfileCommand.class,
})
@Singleton
public class CliConfigSubCommand extends BaseCommand implements Callable<Integer> {

    @Option(names = {"--profile", "-p"})
    private String profileName;

    @Inject
    private CliConfigCall call;

    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new StringCallInput(profileName))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new ProfileDecorator())
                .build()
                .runPipeline();
    }
}
