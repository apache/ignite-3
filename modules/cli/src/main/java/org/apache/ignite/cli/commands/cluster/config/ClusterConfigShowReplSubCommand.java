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

package org.apache.ignite.cli.commands.cluster.config;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.decorators.JsonDecorator;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.exception.handler.IgniteCliApiExceptionHandler;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.rest.client.invoker.ApiException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster in REPL mode.
 */
@Command(name = "show",
        description = "Shows cluster configuration.")
public class ClusterConfigShowReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Node url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to Ignite node."
    )
    private String clusterUrl;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private Session session;

    @Override
    public void run() {
        var input = ClusterConfigShowCallInput.builder().selector(selector);
        if (session.isConnectedToNode()) {
            input.clusterUrl(session.nodeUrl());
        } else if (clusterUrl != null) {
            input.clusterUrl(clusterUrl);
        } else {
            spec.commandLine().getErr().println("You are not connected to node. Run 'connect' command or use '--cluster-url' option.");
            return;
        }

        CallExecutionPipeline.builder(call)
                .inputProvider(input::build)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator())
                .exceptionHandler(new ShowConfigReplExceptionHandler())
                .build()
                .runPipeline();
    }

    private static class ShowConfigReplExceptionHandler extends IgniteCliApiExceptionHandler {
        @Override
        public int handle(ExceptionWriter err, IgniteCliApiException e) {
            if (e.getCause() instanceof ApiException) {
                ApiException apiException = (ApiException) e.getCause();
                if (apiException.getCode() == 500) { //TODO: https://issues.apache.org/jira/browse/IGNITE-17091
                    err.write("Cannot show cluster config, probably you have not initialized the cluster. "
                            + "Try to run 'cluster init' command.");
                    return 1;
                }
            }
            return super.handle(err, e);
        }
    }
}
