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

package org.apache.ignite.migrationtools.cli.persistence.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.migrationtools.cli.exceptions.DataStreamerExceptionHandler;
import org.apache.ignite.migrationtools.cli.exceptions.DefaultMigrateCacheExceptionHandler;
import org.apache.ignite.migrationtools.cli.exceptions.ErrorLoadingInputConfigurationHandlers;
import org.apache.ignite.migrationtools.cli.exceptions.IgniteClientConnectionExceptionHandler;
import org.apache.ignite.migrationtools.cli.persistence.calls.MigrateCacheCall;
import org.apache.ignite.migrationtools.cli.persistence.calls.RetriableMigrateCacheCall;
import org.apache.ignite.migrationtools.cli.persistence.params.IgniteClientAuthenticatorParams;
import org.apache.ignite.migrationtools.cli.persistence.params.MigrateCacheParams;
import org.apache.ignite.migrationtools.cli.persistence.params.RetrieableMigrateCacheParams;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite3.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import picocli.CommandLine;

/** Migrate cache command. */
@CommandLine.Command(
        name = "migrate-cache",
        description = "Migrate a cache from a Ignite 2 work dir into a Ignite 3 cluster"
)
public class MigrateCacheCmd extends BaseCommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    private PersistenceBaseCmd parent;

    @CommandLine.Mixin
    private MigrateCacheParams migrateCacheParams;

    @CommandLine.Mixin
    private RetrieableMigrateCacheParams retryParams;

    @CommandLine.Mixin
    private IgniteClientAuthenticatorParams clientAuthenticatorParams;

    @Override
    public Integer call() throws Exception {
        var migrateCacheCall = new MigrateCacheCall();
        var call = new RetriableMigrateCacheCall(migrateCacheCall);
        return runPipeline(
                CallExecutionPipeline.builder(call)
                        .defaultExceptionHandler(new DefaultExceptionHandlers(DefaultMigrateCacheExceptionHandler.INSTANCE))
                        .exceptionHandler(new IgniteClientConnectionExceptionHandler())
                        .exceptionHandler(new DataStreamerExceptionHandler())
                        .exceptionHandlers(ErrorLoadingInputConfigurationHandlers.create())
                        .inputProvider(() -> new RetriableMigrateCacheCall.Input(
                                parent.params(),
                                migrateCacheParams,
                                retryParams,
                                clientAuthenticatorParams)
                        )
                        .decorator(ouput -> () -> {
                            List<String> parts = new ArrayList<>(2);
                            if (StringUtils.isNotBlank(ouput.getMsg())) {
                                parts.add(ouput.getMsg());
                            }
                            if (ouput.getProgressFilePath() != null) {
                                parts.add("Progress File: " + ouput.getProgressFilePath());
                            }
                            return String.join("\n", parts);
                        })
        );
    }
}
