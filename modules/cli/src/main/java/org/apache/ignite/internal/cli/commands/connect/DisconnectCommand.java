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
import org.apache.ignite.internal.cli.call.connect.DisconnectCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.EmptyCallInput;
import picocli.CommandLine.Command;

/**
 * Connects to the Ignite 3 node.
 */
@Command(name = "disconnect", description = "Disconnects from Ignite 3 node")
public class DisconnectCommand extends BaseCommand implements Runnable {
    @Inject
    private DisconnectCall call;

    /** {@inheritDoc} */
    @Override
    public void run() {
        runPipeline(CallExecutionPipeline.builder(call)
                .inputProvider(EmptyCallInput::new)
        );
    }
}
