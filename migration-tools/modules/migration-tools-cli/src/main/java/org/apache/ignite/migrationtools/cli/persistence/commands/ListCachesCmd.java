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

import java.util.concurrent.Callable;
import org.apache.ignite.migrationtools.cli.exceptions.ErrorLoadingInputConfigurationHandlers;
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
                .exceptionHandlers(ErrorLoadingInputConfigurationHandlers.create())
                .inputProvider(() -> new ListCachesCall.Input(parent.params()))
                .decorator(data -> () -> PlainTableRenderer.render(new String[] {"id", "name"},
                        data.stream().map(o -> new String[] {String.valueOf(o.cacheId()), o.cacheName()}).toArray(Object[][]::new)))
        );
    }
}
