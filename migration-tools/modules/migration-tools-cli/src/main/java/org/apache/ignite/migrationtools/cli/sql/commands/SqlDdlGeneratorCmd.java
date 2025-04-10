/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.sql.commands;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.mixins.ClassloaderOption;
import org.apache.ignite.migrationtools.cli.sql.calls.SqlDdlGeneratorCall;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine;

/** SQL Generator command. */
@CommandLine.Command(
        name = "sql-ddl-generator",
        description = "Generates a SQL DDL Script from the cache configurations in the input Ignite 2 configuration xml")
public class SqlDdlGeneratorCmd extends BaseCommand implements Callable<Integer> {
    @CommandLine.Parameters(paramLabel = "input-file", description = "Ignite 2 or Gridgain 8 Configuration XML")
    private Path inputFile;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Print the DDL Script to a file instead of the STDOUT")
    private Path targetFile;

    @CommandLine.Option(
            names = {"--stop-on-error"},
            description = "Panics on error. By default, skips errors and prints the SQL DDL Script only for the successful caches.")
    private boolean stopOnError;

    @CommandLine.Option(
            names = {"--allow-extra-fields"},
            description = "Add an extra column to serialize unsupported attributes."
                    + " The resulting tables will be compatible with the 'migrate-cache's 'PACK_EXTRA' mode."
                    + " For more information, please checkout the 'allowExtraFields' method in the IgniteAdapter module.")
    private boolean allowExtraFields;

    @CommandLine.Mixin
    private ClassloaderOption classloaderOption;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        var call = new SqlDdlGeneratorCall();
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(() -> new SqlDdlGeneratorCall.Input(
                        inputFile,
                        targetFile,
                        stopOnError,
                        allowExtraFields,
                        classloaderOption.clientClassLoader()))
        );
    }
}
