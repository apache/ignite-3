/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.commands;

import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.persistence.calls.ListCachesCall;
import org.apache.ignite3.internal.cli.commands.BaseCommand;
import org.apache.ignite3.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite3.internal.cli.util.PlainTableRenderer;
import picocli.CommandLine;

/** List Caches command. */
@CommandLine.Command(
        name = "list-caches",
        description = "List available caches on the node"
)
public class ListCachesCmd extends BaseCommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    private PersistenceBaseCmd parent;

    @Override
    public Integer call() {
        var call = new ListCachesCall();
        return runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(() -> new ListCachesCall.Input(parent.params()))
                .decorator(data -> () -> PlainTableRenderer.render(new String[] {"id", "name"},
                        data.stream().map(o -> new String[] {String.valueOf(o.cacheId()), o.cacheName()}).toArray(Object[][]::new)))
        );
    }
}
