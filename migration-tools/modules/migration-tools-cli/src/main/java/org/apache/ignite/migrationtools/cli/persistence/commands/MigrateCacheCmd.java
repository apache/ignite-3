/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.migrationtools.cli.exceptions.IgniteClientConnectionExceptionHandler;
import org.apache.ignite.migrationtools.cli.persistence.calls.MigrateCacheCall;
import org.apache.ignite.migrationtools.cli.persistence.calls.RetriableMigrateCacheCall;
import org.apache.ignite.migrationtools.cli.persistence.params.MigrateCacheParams;
import org.apache.ignite.migrationtools.cli.persistence.params.RetrieableMigrateCacheParams;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
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

    @Override
    public Integer call() throws Exception {
        var migrateCacheCall = new MigrateCacheCall();
        var call = new RetriableMigrateCacheCall(migrateCacheCall);
        return runPipeline(
                CallExecutionPipeline.builder(call)
                        .exceptionHandler(new IgniteClientConnectionExceptionHandler())
                        .inputProvider(() -> new RetriableMigrateCacheCall.Input(parent.params(), migrateCacheParams, retryParams))
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
