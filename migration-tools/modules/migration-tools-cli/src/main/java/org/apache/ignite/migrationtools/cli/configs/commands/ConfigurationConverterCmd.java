/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.configs.commands;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.configs.calls.ConfigurationConverterCall;
import org.apache.ignite.migrationtools.cli.mixins.ClassloaderOption;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine;

/** ConfigurationConverterCmd. */
@CommandLine.Command(
        name = "configuration-converter",
        description = "Converters Ignite 2 configuration xml into Ignite 3 node and cluster configurations")
public class ConfigurationConverterCmd extends BaseCommand implements Callable<Integer> {
    @CommandLine.Parameters(paramLabel = "input-file", description = "Ignite 2 or Gridgain 8 Configuration XML")
    private Path inputFile;

    @CommandLine.Parameters(paramLabel = "node-cfg-output-file", description = "Ignite 3 or Gridgain 9 Configuration HOCON")
    private Path locCfgFile;

    @CommandLine.Parameters(paramLabel = "cluster-cfg-output-file", description = "Ignite 3 or Gridgain 9 Configuration HOCON")
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
                .inputProvider(() -> new ConfigurationConverterCall.Input(inputFile, locCfgFile, distCfgFile, includeDefaults,
                        classloaderOption.clientClassLoader()))
        );
    }
}
