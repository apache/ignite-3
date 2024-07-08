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

package org.apache.ignite.internal.cli.commands.cluster.config;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.decorators.JsonDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

/**
 * Command that shows configuration from the cluster.
 */
@Command(name = "show", description = "Shows cluster configuration")
public class ClusterConfigShowCommand extends BaseCommand implements Callable<Integer> {
    /**
     * Cluster endpoint URL option.
     */
    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    /** Configuration selector option. */
    @Parameters(arity = "0..1", description = "Configuration path selector")
    private String selector;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(this::buildCallInput)
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new JsonDecorator(isHighlightEnabled()))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot show cluster config"))
                .verbose(verbose)
                .build()
                .runPipeline();
    }

    private boolean isHighlightEnabled() {
        return Boolean.parseBoolean(configManagerProvider.get().getCurrentProperty(CliConfigKeys.SYNTAX_HIGHLIGHTING.value()));
    }

    private ClusterConfigShowCallInput buildCallInput() {
        return ClusterConfigShowCallInput.builder()
                .clusterUrl(clusterUrl.getClusterUrl())
                .selector(selector)
                .build();
    }
}
