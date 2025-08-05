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

package org.apache.ignite.migrationtools.cli.configs.commands;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.configs.calls.ConfigurationConverterCall;
import org.apache.ignite.migrationtools.cli.exceptions.ErrorLoadingInputConfigurationHandlers;
import org.apache.ignite.migrationtools.cli.mixins.ClassloaderOption;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine;

/** ConfigurationConverterCmd. */
@CommandLine.Command(
        name = "configuration-converter",
        description = "Converters Ignite 2 configuration xml into Ignite 3 node and cluster configurations")
public class ConfigurationConverterCmd extends BaseCommand implements Callable<Integer> {
    @CommandLine.Parameters(paramLabel = "input-file", description = "Ignite 2 Configuration XML")
    private Path inputFile;

    @CommandLine.Parameters(paramLabel = "node-cfg-output-file", description = "Ignite 3 Configuration HOCON")
    private Path locCfgFile;

    @CommandLine.Parameters(paramLabel = "cluster-cfg-output-file", description = "Ignite 3 Configuration HOCON")
    private Path distCfgFile;

    @CommandLine.Option(
            names = {"-d", "--include-defaults"},
            description = "Whether default values should also be written to the output files.")
    private boolean includeDefaults = false;

    @CommandLine.Mixin
    private ClassloaderOption classloaderOption;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        var call = new ConfigurationConverterCall();
        return runPipeline(CallExecutionPipeline.builder(call)
                .exceptionHandlers(ErrorLoadingInputConfigurationHandlers.create())
                .inputProvider(() -> new ConfigurationConverterCall.Input(inputFile, locCfgFile, distCfgFile, includeDefaults,
                        classloaderOption.clientClassLoader()))
        );
    }
}
