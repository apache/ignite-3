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

package org.apache.ignite.migrationtools.cli.sql.commands;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.exceptions.ErrorLoadingInputConfigurationHandlers;
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
    @CommandLine.Parameters(paramLabel = "input-file", description = "Ignite 2 Configuration XML")
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
                .exceptionHandlers(ErrorLoadingInputConfigurationHandlers.create())
                .inputProvider(() -> new SqlDdlGeneratorCall.Input(
                        inputFile,
                        targetFile,
                        stopOnError,
                        allowExtraFields,
                        classloaderOption.clientClassLoader()))
        );
    }
}
