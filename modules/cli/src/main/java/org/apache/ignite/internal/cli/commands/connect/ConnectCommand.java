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

package org.apache.ignite.internal.cli.commands.connect;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.ReplManager;
import org.apache.ignite.internal.cli.call.connect.ConnectWizardCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "connect", description = "Connects to Ignite 3 node")
public class ConnectCommand extends BaseCommand implements Callable<Integer> {
    @Mixin
    private ConnectOptions connectOptions;

    @Inject
    private ConnectWizardCall connectCall;

    @Inject
    private Provider<ReplManager> replManagerProvider;

    @Override
    public Integer call() {
        ReplManager replManager = replManagerProvider.get();
        // We need to do this before the connect call since it will fire events even before repl start.
        replManager.subscribe();

        int exitCode = runPipeline(CallExecutionPipeline.builder(connectCall)
                .inputProvider(connectOptions::buildCallInput)
        );
        if (exitCode == 0) {
            replManager.startReplMode();
        }
        return exitCode;
    }
}
